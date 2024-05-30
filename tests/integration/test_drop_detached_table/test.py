# Tag no-fasttest: requires S3

import logging
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

replica1 = cluster.add_instance(
    "replica1",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "replica1"},
)
replica2 = cluster.add_instance(
    "replica2",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "replica2"},
)

node_s3 = cluster.add_instance(
    "node_s3",
    main_configs=["configs/disk_s3.xml"],
    with_minio=True,
)


def list_objects(cluster, path="data/", hint="list_objects"):
    minio = cluster.minio_client
    objects = list(minio.list_objects(cluster.minio_bucket, path, recursive=True))
    logging.info(f"{hint} ({len(objects)}): {[x.object_name for x in objects]}")
    return objects


def check_exists(zk, path):
    zk.sync(path)
    return zk.exists(path)


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
    engine = (
        f"ReplicatedMergeTree('/clickhouse/tables/shard1/{table_name}', '{{replica}}')"
    )

    node.query_with_retry(
        f"""
        CREATE TABLE {table_name} ON CLUSTER test_cluster
        (
            number UInt64
        ) 
        ENGINE={engine}
        ORDER BY number
        """
    )


def create_s3_table(node, table_name):
    node.query_with_retry(
        """
        CREATE TABLE {table_name} 
        (
            number UInt64
        ) 
        ENGINE=MergeTree
        ORDER BY number
        SETTINGS storage_policy='s3'
        """.format(
            table_name=table_name
        )
    )


def test_drop_replicated_table(start_cluster):
    objects_before = list_objects(cluster, "data/")

    create_replicated_table(node=replica1, table_name="test_replicated_table")

    replica1.query(
        "INSERT INTO test_replicated_table SELECT number FROM system.numbers LIMIT 6;"
    )
    replica1.query("SYSTEM SYNC REPLICA test_replicated_table;", timeout=20)

    replica1.query(
        "DETACH TABLE test_replicated_table ON CLUSTER test_cluster PERMANENTLY;"
    )

    zk = cluster.get_kazoo_client("zoo1")

    exists_replica_1_1 = check_exists(
        zk,
        "/clickhouse/tables/shard1/{table_name}/replicas/{replica}".format(
            table_name="test_replicated_table", replica=replica1.name
        ),
    )
    assert exists_replica_1_1 != None

    replica1.query(
        "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE test_replicated_table ON CLUSTER test_cluster SYNC;"
    )

    objects_after = list_objects(cluster, "data/")
    assert len(objects_before) == len(objects_after)

    exists_node = check_exists(
        zk,
        "/clickhouse/tables/shard1/{table_name}/".format(
            table_name="test_replicated_table"
        ),
    )
    assert exists_node == None


def test_drop_s3_table(start_cluster):
    objects_before = list_objects(cluster, "data/")

    create_s3_table(node_s3, "test_s3_table")

    node_s3.query(
        "INSERT INTO test_s3_table SELECT number FROM system.numbers LIMIT 6;"
    )

    node_s3.query("DETACH TABLE test_s3_table PERMANENTLY;")
    node_s3.query(
        "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE test_s3_table SYNC;"
    )

    objects_after = list_objects(cluster, "data/")
    assert len(objects_before) == len(objects_after)
