import pytest
import threading
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    user_configs=["configs/users.xml"],
    stay_alive=True,
    macros={"shard": "s1", "replica": "r1"},
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    user_configs=["configs/users.xml"],
    stay_alive=True,
    macros={"shard": "s1", "replica": "r2"},
)
node3 = cluster.add_instance(
    "node3",
    with_zookeeper=True,
    user_configs=["configs/users.xml"],
    stay_alive=True,
    macros={"shard": "s1", "replica": "r3"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_last_block_number_for_partition(node, table_uuid, partition):
    query = f"""
    SELECT 
        toInt64(replaceRegexpOne(name, 'block-', '')) as block_number
    FROM system.zookeeper
    WHERE path = '/clickhouse/tables/{table_uuid}/s1/block_numbers/{partition}'
        AND name LIKE 'block-%'
    ORDER BY block_number DESC
    LIMIT 1
    """
    result = node.query(query).strip()
    if result == "":
        return None
    return int(result)


def wait_for_queue_to_process(node, database, table, timeout=60):
    """Poll until replication queue is empty."""
    start = time.time()
    while time.time() - start < timeout:
        result = node.query(
            f"""
            SELECT queue_size, absolute_delay 
            FROM system.replicas 
            WHERE database = '{database}' AND table = '{table}'
        """
        )

        if result:
            queue_size, delay = map(int, result.strip().split("\t"))
            if queue_size == 0 and delay == 0:
                print(f"Queue processed on {node.name}")
                return True

        time.sleep(0.5)

    raise TimeoutError(f"Queue not processed within {timeout}s")


def test_partition_move_drop_race(started_cluster):
    for node in [node1, node2, node3]:
        node.query(f"DROP DATABASE IF EXISTS test_db SYNC")

    node1.query("SYSTEM ENABLE FAILPOINT rmt_delay_execute_drop_range")
    node1.query("SYSTEM ENABLE FAILPOINT rmt_delay_commit_part")

    for node in [node1, node2, node3]:
        node.query(
            """
            CREATE DATABASE test_db 
            ENGINE = Replicated('/test/db', '{shard}', '{replica}')
        """
        )

    node1.query(
        f"""
        CREATE TABLE test_db.tbl (id UInt32, val String)
        ENGINE = ReplicatedMergeTree
        PARTITION BY id % 10 ORDER BY id
        SETTINGS old_parts_lifetime = 1
    """
    )

    for node in [node1, node2, node3]:
        assert node.query_with_retry(
            "SELECT count() FROM system.tables WHERE database='test_db' AND name='tbl'",
            check_callback=lambda x: x.strip() == "1",
        )

    table_uuid = node1.query(
        "SELECT uuid FROM system.tables WHERE database='test_db' AND name='tbl'"
    ).strip()

    node2.query(f"SYSTEM STOP FETCHES test_db.tbl")
    node3.query(f"SYSTEM STOP FETCHES test_db.tbl")

    prev_block_number = get_last_block_number_for_partition(node1, table_uuid, 0)

    exception_holder = [None]
    def drop_op():
        try:
            # Wait until INSERT query allocates the block number
            timeout = 60  # seconds
            start_time = time.time()

            while True:
                current_block_number = get_last_block_number_for_partition(
                    node1, table_uuid, 0
                )
                if current_block_number != prev_block_number:
                    break

                if time.time() - start_time > timeout:
                    raise TimeoutError(
                        f"Timeout waiting for block number to change after {timeout}s. "
                        f"Previous: {prev_block_number}, Current: {current_block_number}"
                    )
                time.sleep(0.5)

            node1.query(f"ALTER TABLE test_db.tbl DROP PARTITION 0")
        except Exception as e:
            exception_holder[0] = e

    t = threading.Thread(target=drop_op)
    t.start()

    node1.query(f"INSERT INTO test_db.tbl VALUES (0, 'a'), (10, 'b'), (20, 'c')")
    t.join()

    if exception_holder[0]:
        raise exception_holder[0]

    wait_for_queue_to_process(node1, "test_db", "tbl")
    node3.query(f"SYSTEM START FETCHES test_db.tbl")
    wait_for_queue_to_process(node3, "test_db", "tbl")

    errors = []
    for node in [node1, node2, node3]:
        lost = int(
            node.query(
                "SELECT lost_part_count FROM system.replicas WHERE database = 'test_db' AND table = 'tbl'"
            ).strip()
        )

        print(f"{node.name} lost_parts: {lost}")

        if lost > 0:
            errors.append(f"{node.name} has {lost} lost parts")

    if errors:
        pytest.fail(f"Race: {'; '.join(errors)}")

    for node in [node1, node2, node3]:
        node.query(f"DROP DATABASE IF EXISTS test_db SYNC")
