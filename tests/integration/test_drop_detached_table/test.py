# Tag no-fasttest: requires S3

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

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


def assert_zk_node_not_exists(
    zk,
    node_path,
    retry_count=20,
    sleep_time=0.5,
):
    for i in range(retry_count):
        try:
            exists_replica = check_exists(
                zk,
                node_path,
            )
            if exists_replica != None:
                return
            time.sleep(sleep_time)
        except Exception as ex:
            logging.exception(f"assert_zk_node_not_exists retry {i+1} exception {ex}")
            time.sleep(sleep_time)

    assert exists_replica == None


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


def create_table(node, table_name):
    node.query(f"CREATE TABLE {table_name} (key UInt64) ENGINE=MergeTree ORDER BY key")


def create_distributed_table(node, table_name):
    node.query(
        "CREATE TABLE aux_table_for_dist (key UInt64) ENGINE=MergeTree ORDER BY key"
    )
    node.query(
        f"CREATE TABLE {table_name} AS aux_table_for_dist Engine=Distributed('test_cluster', test_db, '{table_name}', key)"
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


def check_no_table_in_detached_table(node, table_name: str):
    query = (
        f"SELECT count(table) FROM system.detached_tables WHERE table='{table_name}'"
    )
    assert_eq_with_retry(instance=node, query=query, expectation="0")


def test_drop_table_with_detached_flag(start_cluster):
    table_name = "test_table"
    create_table(replica1, table_name)

    disk_path = replica1.query("select path from system.disks").strip()
    metadata_path = replica1.query(
        f"SELECT metadata_path FROM system.tables WHERE table='{table_name}'"
    ).split()[0]

    replica1.query(f"DETACH TABLE {table_name} PERMANENTLY")

    assert (
        "1"
        == replica1.exec_in_container(
            ["bash", "-c", f"ls -1 {disk_path}{metadata_path} | wc -l"]
        ).strip()
    )
    assert (
        "1"
        == replica1.exec_in_container(
            ["bash", "-c", f"ls -1 {disk_path}{metadata_path}.detached | wc -l"]
        ).strip()
    )

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {table_name} SYNC;"
    )

    assert (
        "0"
        == replica1.exec_in_container(
            ["bash", "-c", f"ls -1 {disk_path}{metadata_path} | wc -l"]
        ).strip()
    )
    assert (
        "0"
        == replica1.exec_in_container(
            ["bash", "-c", f"ls -1 {disk_path}{metadata_path}.detached | wc -l"]
        ).strip()
    )


def test_drop_replicated_table(start_cluster):
    objects_before = list_objects(cluster, "data/")
    table_name = "test_replicated_table"

    create_replicated_table(node=replica1, table_name=table_name)

    replica1.query(
        f"INSERT INTO {table_name} SELECT number FROM system.numbers LIMIT 6;"
    )
    replica1.query(f"SYSTEM SYNC REPLICA {table_name};", timeout=20)

    replica1.query(f"DETACH TABLE {table_name} ON CLUSTER test_cluster PERMANENTLY;")

    zk = cluster.get_kazoo_client("zoo1")

    zk_path_replica1_node = (
        f"/clickhouse/tables/shard1/{table_name}/replicas/{replica1.name}"
    )
    zk_path_replica2_node = (
        f"/clickhouse/tables/shard1/{table_name}/replicas/{replica2.name}"
    )

    exists_replica = check_exists(
        zk,
        zk_path_replica1_node,
    )
    assert exists_replica != None

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {table_name} SYNC;"
    )

    check_no_table_in_detached_table(node=replica1, table_name=table_name)
    assert_zk_node_not_exists(zk, zk_path_replica1_node)

    exists_replica = check_exists(
        zk,
        zk_path_replica2_node,
    )
    assert exists_replica != None

    replica2.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {table_name} SYNC;"
    )

    check_no_table_in_detached_table(node=replica2, table_name=table_name)
    assert_zk_node_not_exists(zk, zk_path_replica2_node)

    objects_after = list_objects(cluster, "data/")
    assert len(objects_before) == len(objects_after)


def test_drop_s3_table(start_cluster):
    objects_before = list_objects(cluster, "data/")
    table_name = "test_replicated_table"

    create_s3_table(node_s3, table_name)

    node_s3.query(
        f"INSERT INTO {table_name} SELECT number FROM system.numbers LIMIT 6;"
    )

    node_s3.query(f"DETACH TABLE {table_name} PERMANENTLY;")
    node_s3.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {table_name} SYNC;"
    )

    check_no_table_in_detached_table(node=node_s3, table_name=table_name)

    objects_after = list_objects(cluster, "data/")
    assert len(objects_before) == len(objects_after)


def test_drop_distributed_table(start_cluster):
    test_table_name = "test_drop_distributed_table"
    create_distributed_table(node=replica1, table_name=test_table_name)

    replica1.query(f"DETACH TABLE {test_table_name}")

    assert (
        "1"
        == replica1.query(
            f"SELECT count(table) FROM system.detached_tables WHERE table='{test_table_name}'"
        ).rstrip()
    )

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {test_table_name} SYNC;"
    )

    check_no_table_in_detached_table(node=replica1, table_name=test_table_name)

    assert (
        "0"
        == replica1.query(
            f"SELECT count(table) FROM system.detached_tables WHERE table='{test_table_name}'"
        ).rstrip()
    )

    replica1.query("DROP TABLE aux_table_for_dist SYNC")
