import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_ddl_queue_delete_add_replica(started_cluster):
    #  Some query started on the cluster, then we deleted some unfinished node
    #  and added a new node to the cluster. Considering that there are less
    #  finished nodes than expected and we can't resolve deleted node's hostname
    #  the queue will be stuck on a new node.
    #  <host_name> inside <distributed_ddl> allows us to simply discard deleted
    #  node's hostname by simple comparison without trying to resolve it.

    node1.query(
        "create table hostname_change on cluster test_cluster (n int) engine=Log"
    )

    # There's no easy way to change hostname of a container, so let's update values in zk
    query_znode = node1.query(
        "select max(name) from system.zookeeper where path='/clickhouse/task_queue/ddl'"
    )[:-1]

    value = (
        node1.query(
            f"select value from system.zookeeper where path='/clickhouse/task_queue/ddl' and name='{query_znode}' format TSVRaw"
        )[:-1]
        .replace(
            "hosts: ['node1:9000']", "hosts: ['finished_node:9000','deleted_node:9000']"
        )
        .replace("initiator: node1:9000", "initiator: finished_node:9000")
        .replace("\\'", "#")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("#", "\\'")
    )

    finished_znode = node1.query(
        f"select name from system.zookeeper where path='/clickhouse/task_queue/ddl/{query_znode}/finished' and name like '%node1%'"
    )[:-1]

    node1.query(
        f"insert into system.zookeeper (name, path, value) values ('{query_znode}', '/clickhouse/task_queue/ddl', '{value}')"
    )
    started_cluster.get_kazoo_client("zoo1").delete(
        f"/clickhouse/task_queue/ddl/{query_znode}/finished/{finished_znode}"
    )

    finished_znode = finished_znode.replace("node1", "finished_node")

    node1.query(
        f"insert into system.zookeeper (name, path, value) values ('{finished_znode}', '/clickhouse/task_queue/ddl/{query_znode}/finished', '0\\n')"
    )

    node1.restart_clickhouse(kill=True)

    node1.query(
        "create table hostname_change2 on cluster test_cluster (n int) engine=Log"
    )
