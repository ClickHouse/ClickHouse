from pathlib import Path
import time
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "config.xml",
    ],
    macros={"replica": "node1"},
    with_zookeeper=True,
    with_minio=True,
)

DATABASE_NAME = "stale_moving_parts"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=DATABASE_NAME, sql=query)


# .../disks/s3/store/
def get_table_path(node, table):
    return (
        node.query(
            sql=f"SELECT data_paths FROM system.tables WHERE table = '{table}' and database = '{DATABASE_NAME}' LIMIT 1"
        )
        .strip('"\n[]')
        .split(",")[1]
        .strip("'")
    )


def exec(node, cmd, path):
    return node.exec_in_container(
        [
            "bash",
            "-c",
            f"{cmd} {path}",
        ]
    )


def stop_zookeeper(node):
    node.exec_in_container(["bash", "-c", "/opt/zookeeper/bin/zkServer.sh stop"])
    timeout = time.time() + 60
    while node.get_process_pid("zookeeper") != None:
        if time.time() > timeout:
            raise Exception("Failed to stop ZooKeeper in 60 secs")
        time.sleep(0.2)


def wait_part_is_stuck(node, table_moving_path, moving_part):
    num_tries = 5
    while q(node, "SELECT part_name FROM system.moves").strip() != moving_part:
        if num_tries == 0:
            raise Exception("Part has not started to move")
        num_tries -= 1
        time.sleep(1)
    num_tries = 5
    while exec(node, "ls", table_moving_path).strip() != moving_part:
        if num_tries == 0:
            raise Exception("Part is not stuck in the moving directory")
        num_tries -= 1
        time.sleep(1)


def wait_zookeeper_node_to_start(zk_nodes, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            for instance in zk_nodes:
                conn = cluster.get_kazoo_client(instance)
                conn.get_children("/")
            print("All instances of ZooKeeper started")
            return
        except Exception as ex:
            print(("Can't connect to ZooKeeper " + str(ex)))
            time.sleep(0.5)


def test_remove_stale_moving_parts_without_zookeeper(started_cluster):
    ch1.query(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

    q(
        ch1,
        "CREATE TABLE test_remove ON CLUSTER cluster ( id UInt32 ) ENGINE ReplicatedMergeTree() ORDER BY id;",
    )

    table_moving_path = Path(get_table_path(ch1, "test_remove")) / "moving"

    q(ch1, "SYSTEM ENABLE FAILPOINT stop_moving_part_before_swap_with_active")
    q(ch1, "INSERT INTO test_remove SELECT number FROM numbers(100);")
    moving_part = "all_0_0_0"
    move_response = ch1.get_query_request(
        sql=f"ALTER TABLE test_remove MOVE PART '{moving_part}' TO DISK 's3'",
        database=DATABASE_NAME,
    )

    wait_part_is_stuck(ch1, table_moving_path, moving_part)

    cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    # Stop moves in case table is not read-only yet
    q(ch1, "SYSTEM STOP MOVES")
    q(ch1, "SYSTEM DISABLE FAILPOINT stop_moving_part_before_swap_with_active")

    assert "Cancelled moving parts" in move_response.get_error()
    assert exec(ch1, "ls", table_moving_path).strip() == ""

    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    wait_zookeeper_node_to_start(["zoo1", "zoo2", "zoo3"])
    q(ch1, "SYSTEM START MOVES")

    q(ch1, f"DROP TABLE test_remove")
