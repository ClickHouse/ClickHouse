import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node4 = cluster.add_instance(
    "node4",
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


def test_ddl_worker_replicas(started_cluster):
    for replica in ["node1:9000", "node2:9000", "node3:9000", "node4:9000"]:
        # wait until the replicas path is created
        node1.query_with_retry(
            sql=f"SELECT count() FROM system.zookeeper WHERE path='/clickhouse/task_queue/replicas/{replica}'",
            check_callback=lambda result: result == 1,
        )

        result = node1.query(
            f"SELECT name, value, ephemeralOwner FROM system.zookeeper WHERE path='/clickhouse/task_queue/replicas/{replica}'"
        ).strip()
        print(f"result: {replica} {result}")

        lines = list(result.split("\n"))
        assert len(lines) == 1
        parts = list(lines[0].split("\t"))
        assert len(parts) == 3
        assert parts[0] == "active"
        assert len(parts[1]) != 0
        assert len(parts[2]) != 0

    try:
        node4.stop_clickhouse()

        # wait for node4 active path is removed
        node1.query_with_retry(
            sql=f"SELECT count() FROM system.zookeeper WHERE path='/clickhouse/task_queue/replicas/node4:9000'",
            check_callback=lambda result: result == 0,
        )

        result = node1.query_with_retry(
            f"SELECT name, value, ephemeralOwner FROM system.zookeeper WHERE path='/clickhouse/task_queue/replicas/node4:9000'"
        ).strip()

        print(f"result: {replica} {result}")

        lines = list(result.split("\n"))
        assert len(lines) == 1
        assert len(lines[0]) == 0
    finally:
        node4.start_clickhouse()
