# Tag no-fasttest: requires S3

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

replica1 = cluster.add_instance(
    "replica1", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)
replica2 = cluster.add_instance(
    "replica2", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)

node_s3 = cluster.add_instance(
    "node_s3",
    main_configs=["configs/disk_s3.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def create_replicated_table(node, table_name):
    replica = node.name
    engine = f"ReplicatedMergeTree('/clickhouse/tables/1/{table_name}', '{replica}')"

    node.query_with_retry(
        """
        CREATE TABLE {table_name} 
        (
            number UInt64
        ) 
        ENGINE={engine}
        ORDER BY number
        """.format(
            table_name=table_name, engine=engine
        )
    )


def create_s3_table(node, table_name):
    engine = "MergeTree"

    node.query_with_retry(
        """
        CREATE TABLE {table_name} 
        (
            number UInt64
        ) 
        ENGINE={engine}
        ORDER BY number
        SETTINGS storage_policy='s3'
        """.format(
            table_name=table_name, engine=engine
        )
    )


def test_drop_replicated_table(start_cluster):
    create_replicated_table(replica1, "test_replicated_table")
    create_replicated_table(replica2, "test_replicated_table")

    replica1.query(
        "INSERT INTO test_replicated_table SELECT number FROM system.numbers LIMIT 6;"
    )
    replica1.query("SYSTEM SYNC REPLICA test_replicated_table;", timeout=20)

    replica1.query("DETACH TABLE test_replicated_table PERMANENTLY;")
    replica1.query(
        "SET allow_drop_detached_table=1; DROP TABLE test_replicated_table SYNC;"
    )

    replica2.query("DETACH TABLE test_replicated_table PERMANENTLY;")
    replica2.query(
        "SET allow_drop_detached_table=1; DROP TABLE test_replicated_table SYNC;"
    )


def test_drop_s3_table(start_cluster):
    create_s3_table(node_s3, "test_s3_table")
    node_s3.query(
        "INSERT INTO test_s3_table SELECT number FROM system.numbers LIMIT 6;"
    )

    node_s3.query("DETACH TABLE test_s3_table PERMANENTLY;")
    node_s3.query("SET allow_drop_detached_table=1; DROP TABLE test_s3_table SYNC;")
