import random
import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def clickhouse_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/storage.xml",
                "configs/disk_connection_limit.xml",
            ],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_both_https(clickhouse_cluster):
    node = clickhouse_cluster.instances["node"]
    node.query(
    """
        DROP TABLE IF EXISTS test_table SYNC;
        CREATE TABLE test_table(
            id UInt32,
            a0 String,
            a1 String,
            a2 String,
            a3 String,
            a4 String,
            a5 String,
            a6 String,
            a7 String,
            a8 String,
            a9 String,
            a10 String,
            a11 String,
            a12 String,
            a13 String,
            a14 String,
            a15 String,
            a16 String,
            a17 String,
            a18 String,
            a19 String)
        ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = 's3', min_bytes_for_wide_part=1000000;
    """)

    node.query("SYSTEM STOP MERGES test_table")

    insert_query_id = f"insert_compact_{int(random.random() * 1000000)}"
    node.query(
    """
        INSERT INTO test_table VALUES (1, '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19');
    """,
    query_id=insert_query_id
    )

    node.query("SYSTEM FLUSH LOGS")
    puts, connections = node.query(
    f"""
        SELECT ProfileEvents['S3PutObject'], ProfileEvents['DiskConnectionsCreated'] FROM system.query_log WHERE query_id = '{insert_query_id}' AND type = 'QueryFinish'
    """
    ).strip().split("\t")
    assert(int(puts) <= 10)
    assert(int(connections) <= 10)

    select_query_id = f"select_compact_{int(random.random() * 1000000)}"
    result = node.query(
    """
        SELECT * FROM test_table WHERE id = 1;
    """,
    query_id=select_query_id
    ).strip()
    assert(result == "1\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t19")

    node.query("SYSTEM FLUSH LOGS")
    gets, connections = node.query(
    f"""
        SELECT ProfileEvents['S3GetObject'], ProfileEvents['DiskConnectionsCreated'] FROM system.query_log WHERE query_id = '{select_query_id}' AND type = 'QueryFinish'
    """
    ).strip().split("\t")
    assert(int(gets) <= 3)
    assert(int(connections) <= 3)

    # Now tests with wide part when connection limit is hit

    node.query("ALTER TABLE test_table MODIFY SETTING min_bytes_for_wide_part=1")

    insert_query_id = f"insert_wide_{int(random.random() * 1000000)}"
    error = node.query_and_get_error(
    """
        INSERT INTO test_table VALUES (2, '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19');
    """,
    query_id=insert_query_id
    )
    assert("HTTP_CONNECTION_LIMIT_REACHED" in error)
