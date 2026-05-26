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
            if exists_replica is None:
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

    node.query_with_retry(f"""
        CREATE TABLE {table_name} ON CLUSTER test_cluster
        (
            number UInt64
        ) 
        ENGINE={engine}
        ORDER BY number
        """)


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
    node.query_with_retry("""
        CREATE TABLE {table_name} 
        (
            number UInt64
        ) 
        ENGINE=MergeTree
        ORDER BY number
        SETTINGS disk='s3'
        """.format(table_name=table_name))


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


def test_drop_detached_on_cluster(start_cluster):
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

    assert (
        "1"
        == replica1.query(
            f"SELECT count(table) FROM system.detached_tables WHERE table='{table_name}'"
        ).rstrip()
    )
    assert (
        "1"
        == replica2.query(
            f"SELECT count(table) FROM system.detached_tables WHERE table='{table_name}'"
        ).rstrip()
    )

    exists_replica = check_exists(zk, zk_path_replica1_node)
    assert exists_replica != None
    exists_replica = check_exists(zk, zk_path_replica2_node)
    assert exists_replica != None

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {table_name} ON CLUSTER test_cluster SYNC;"
    )

    check_no_table_in_detached_table(node=replica1, table_name=table_name)
    check_no_table_in_detached_table(node=replica2, table_name=table_name)
    assert_zk_node_not_exists(zk, zk_path_replica1_node)
    assert_zk_node_not_exists(zk, zk_path_replica2_node)

    objects_after = list_objects(cluster, "data/")
    assert len(objects_before) == len(objects_after)


def test_drop_detached_in_replicated_database(start_cluster):
    table_name = "test_replicated_db_table"

    replica1.query(
        "CREATE DATABASE test_r ENGINE=Replicated('/clickhouse/test_r', 'r1')"
    )
    replica2.query(
        "CREATE DATABASE test_r ENGINE=Replicated('/clickhouse/test_r', 'r2')"
    )

    replica1.query(
        f"CREATE TABLE test_r.{table_name} (key UInt64) ENGINE=MergeTree ORDER BY key"
    )
    replica1.query(
        f"INSERT INTO test_r.{table_name} SELECT number FROM system.numbers LIMIT 10"
    )

    replica1.query(f"DETACH TABLE test_r.{table_name} PERMANENTLY")

    assert (
        "1"
        == replica1.query(
            f"SELECT count(table) FROM system.detached_tables WHERE table='{table_name}'"
        ).rstrip()
    )

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE test_r.{table_name} SYNC"
    )

    check_no_table_in_detached_table(node=replica1, table_name=table_name)
    check_no_table_in_detached_table(node=replica2, table_name=table_name)

    replica1.query("DROP DATABASE test_r SYNC")


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


def test_invalid_syntax_corner_cases(start_cluster):
    table_name = "test_table_reject"
    create_table(replica1, table_name)

    replica1.query(f"DETACH TABLE {table_name}")

    def do_query(q):
        try:
            replica1.query(f"SET allow_experimental_drop_detached_table=1; {q} SYNC")
            assert False, f"Got no error for {q}"
        except AssertionError:
            raise
        except Exception as e:
            assert "Syntax error" in str(e) or "SYNTAX_ERROR" in str(
                e
            ), f"Expected syntax error, got: {e}"

    do_query(f"DETACH DETACHED TABLE {table_name}")
    do_query(f"TRUNCATE DETACHED TABLE {table_name}")
    do_query(f"DROP DETACHED TABLE IF EMPTY {table_name}")
    do_query(f"DROP DETACHED VIEW {table_name}")
    do_query(f"DROP DETACHED DICTIONARY {table_name}")
    do_query(f"DROP DETACHED TABLE TEMPORARY {table_name}")

    replica1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


def test_drop_detached_access_control(start_cluster):
    """Test DROP DETACHED TABLE with access control"""
    table_name = "test_access_table"

    create_table(replica1, table_name)
    replica1.query(f"DETACH TABLE {table_name} PERMANENTLY")

    replica1.query("CREATE USER IF NOT EXISTS test_user")
    replica1.query("GRANT SELECT ON test_db.* TO test_user")

    try:
        replica1.query(
            f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {table_name} SYNC",
            user="test_user",
        )
        assert False, "Expected permission denied error"
    except Exception as e:
        assert (
            "permission" in str(e).lower() or "access" in str(e).lower()
        ), f"Expected permission error, got: {e}"

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {table_name} SYNC"
    )

    replica1.query("DROP USER IF EXISTS test_user")


def test_if_exists_behaviour(start_cluster):
    table_name = "test_table_if_exists"

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE IF EXISTS {table_name} SYNC"
    )

    create_table(replica1, table_name)

    try:
        replica1.query(
            f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE IF EXISTS {table_name} SYNC"
        )
    except Exception as e:
        assert "must be detached" in str(e)

    replica1.query(f"DETACH TABLE {table_name}")
    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE IF EXISTS {table_name} SYNC"
    )

    replica1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
