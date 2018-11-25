from kubernetes import client, config, watch
import json
import random

config.load_kube_config()
schedule_name = "tim"
client.Configuration().host = "https://192.168.99.100:8443"
v1 = client.CoreV1Api()


def get_unscheduled_pods():
    w = watch.Watch()
    unscheduled_pods = []
    for event in w.stream(v1.list_namespaced_pod, "default"):
        if event['object'].status.phase == 'Pending' and event['object'].spec.node_name is None:
            print(event['object'].metadata.name)
            unscheduled_pods.append(event['object'].metadata.name)

    return unscheduled_pods


# def compute_pod_resources():


# def nodes_available():
#     ready_nodes = []
#     for n in v1.list_node().items:
#         for status in n.status.conditions:
#             if status.status == "True" and status.type == "Ready":
#                 ready_nodes.append(n.metadata.name)
#
#     return ready_nodes

def nodes_available(resource):
    ready_nodes = []
    for n in v1.list_node().items:
        if float(n.status.allocatable['cpu']) * 1000 > resource:
            print(n.metadata.name)
            ready_nodes.append(n.metadata.name)

    return ready_nodes


def scheduler(name, node, namespace='default'):
    target = client.V1ObjectReference()
    target.kind = "Node"
    target.api_version = "v1"
    target.name = node

    meta = client.V1ObjectMeta()
    meta.name = name

    body = client.V1Binding(metadata=meta, target=target)
    body.target = target
    body.metadata = meta

    return v1.create_namespaced_binding(namespace, body)


def main():
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "default"):
        if event['object'].status.phase == 'Pending' and event['object'].spec.node_name is None:
            try:
                print(event['object'].metadata.name)

                cpu_required = 0
                for c in event['object'].spec.containers:
                    print(c.resources.requests['cpu'])
                    print(c.resources.requests['memory'])

                    cpu_required += float(c.resources.requests['cpu'][:-1])

                res = scheduler(event['object'].metadata.name, random.choice(nodes_available(cpu_required)))
                print(res)

            except client.rest.ApiException as e:
                print json.load(e.body)["message"]


if __name__ == '__main__':
    main()
