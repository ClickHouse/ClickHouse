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
    stay_alive=True,
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

# These reviewer nits will be addressed after coverage is gathered, so we can
# reveal and cover the whole pack of similar corner cases at once.
# TODO reviewer-on-cluster-access: cover `DROP DETACHED TABLE ... ON CLUSTER`
# uses detached-table `DROP_TABLE` access, not generic `DROP_TABLE | DROP_VIEW`.
# TODO reviewer-metadata-dropped-queue: fault-inject `enqueueDroppedTableCleanup`
# failure after metadata move and verify UUID/local state recovery.
# TODO reviewer-replicated-metadata-mutex: prove detached storage wait does not
# hold replicated database `metadata_mutex`.
# TODO reviewer-replicated-stale-table-map: cover stale `table_in_map` with a
# delayed replicated metadata update.


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
        f"CREATE TABLE {table_name} AS aux_table_for_dist "
        f"Engine=Distributed('test_cluster', test_db, '{table_name}', key)"
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
    assert_detached_table_count(node, table_name, "0")


def detached_table_count_query(table_name):
    return f"SELECT count(table) FROM system.detached_tables WHERE table='{table_name}'"


def assert_detached_table_count(node, table_name, count):
    assert_eq_with_retry(
        instance=node,
        query=detached_table_count_query(table_name),
        expectation=str(count),
    )


def dropped_table_count_query(table_name):
    return f"SELECT count() FROM system.dropped_tables WHERE table = '{table_name}'"


def assert_error_contains(error, *needles):
    assert any(needle in error for needle in needles), f"Unexpected error: {error}"


def create_detached_table(node, table_name, permanently=True, rows=0):
    create_table(node, table_name)
    if rows:
        node.query(
            f"INSERT INTO {table_name} SELECT number FROM system.numbers LIMIT {rows}"
        )
    node.query(f"DETACH TABLE {table_name}{' PERMANENTLY' if permanently else ''}")


def get_replicated_table_zk_paths(table_name):
    return [
        f"/clickhouse/tables/shard1/{table_name}/replicas/{replica1.name}",
        f"/clickhouse/tables/shard1/{table_name}/replicas/{replica2.name}",
    ]


def assert_replicated_detached_table_exists(table_name, zk, zk_paths):
    assert_detached_table_count(replica1, table_name, "1")
    assert_detached_table_count(replica2, table_name, "1")
    for zk_path in zk_paths:
        assert check_exists(zk, zk_path) is not None


def create_force_drop_flag(node):
    force_drop_flag_path = "/var/lib/clickhouse/flags/force_drop_table"
    node.exec_in_container(
        [
            "bash",
            "-c",
            "touch {} && chmod a=rw {}".format(
                force_drop_flag_path, force_drop_flag_path
            ),
        ],
        user="root",
    )


def test_drop_detached_table_moves_metadata_to_dropped_queue(start_cluster):
    table_name = "test_table"
    create_table(replica1, table_name)

    disk_path = replica1.query(
        "SELECT path FROM system.disks WHERE name = 'default'"
    ).strip()
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
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {table_name}"
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

    metadata_dropped_path = replica1.query(
        "SELECT metadata_dropped_path FROM system.dropped_tables "
        f"WHERE table = '{table_name}'"
    ).strip()
    assert metadata_dropped_path.startswith("metadata_dropped/")
    assert (
        "1"
        == replica1.exec_in_container(
            ["bash", "-c", f"ls -1 {disk_path}{metadata_dropped_path} | wc -l"]
        ).strip()
    )


def test_force_drop_flag_bypasses_detached_table_size_limit(start_cluster):
    table_name = "test_force_drop_flag_bypasses_detached_table_size_limit"
    create_detached_table(replica1, table_name, rows=1000)

    error = replica1.query_and_get_error(
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {table_name} SYNC SETTINGS max_table_size_to_drop=1"
    )
    assert "is greater than max" in error

    create_force_drop_flag(replica1)
    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {table_name} SYNC SETTINGS max_table_size_to_drop=1"
    )
    check_no_table_in_detached_table(node=replica1, table_name=table_name)


def test_drop_detached_on_cluster(start_cluster):
    table_name = "test_replicated_table"

    create_replicated_table(node=replica1, table_name=table_name)

    replica1.query(
        f"INSERT INTO {table_name} SELECT number FROM system.numbers LIMIT 6;"
    )
    replica1.query(f"SYSTEM SYNC REPLICA {table_name};", timeout=20)

    replica1.query(f"DETACH TABLE {table_name} ON CLUSTER test_cluster PERMANENTLY;")

    zk = cluster.get_kazoo_client("zoo1")

    zk_paths = get_replicated_table_zk_paths(table_name)
    assert_replicated_detached_table_exists(table_name, zk, zk_paths)

    error = replica1.query_and_get_error(
        f"DROP DETACHED TABLE {table_name} ON CLUSTER test_cluster SYNC"
    )
    assert "allow_experimental_drop_detached_table" in error
    assert_replicated_detached_table_exists(table_name, zk, zk_paths)

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {table_name} ON CLUSTER test_cluster SYNC;"
    )

    check_no_table_in_detached_table(node=replica1, table_name=table_name)
    check_no_table_in_detached_table(node=replica2, table_name=table_name)
    for zk_path in zk_paths:
        assert_zk_node_not_exists(zk, zk_path)


def test_drop_detached_on_cluster_without_local_prevalidation(start_cluster):
    database_name = "test_drop_detached_no_local_prevalidation_db"
    table_name = "test_drop_detached_no_local_prevalidation_table"

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1, distributed_ddl_output_mode='never_throw'; "
        f"DROP DETACHED TABLE {database_name}.{table_name} ON CLUSTER test_cluster"
    )
    replica1.query(f"DROP DATABASE IF EXISTS {database_name}")
    replica2.query(f"DROP DATABASE IF EXISTS {database_name}")


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

    assert_detached_table_count(replica1, table_name, "1")
    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE test_r.{table_name} SYNC"
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
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {table_name} SYNC"
    )

    check_no_table_in_detached_table(node=node_s3, table_name=table_name)

    objects_after = list_objects(cluster, "data/")
    assert len(objects_before) == len(objects_after)


def test_drop_distributed_table(start_cluster):
    test_table_name = "test_drop_distributed_table"
    create_distributed_table(node=replica1, table_name=test_table_name)

    replica1.query(f"DETACH TABLE {test_table_name}")

    assert_detached_table_count(replica1, test_table_name, "1")
    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {test_table_name} SYNC"
    )
    check_no_table_in_detached_table(node=replica1, table_name=test_table_name)

    replica1.query("DROP TABLE aux_table_for_dist SYNC")


def test_drop_detached_access_control(start_cluster):
    """Test DROP DETACHED TABLE with access control"""
    table_name = "test_access_table"

    create_detached_table(replica1, table_name)

    replica1.query("CREATE USER IF NOT EXISTS test_user")
    replica1.query("GRANT SELECT ON test_db.* TO test_user")

    error = replica1.query_and_get_error(
        f"SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE {table_name} SYNC",
        user="test_user",
    )
    assert_error_contains(error.lower(), "permission", "access")

    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {table_name} SYNC"
    )
    replica1.query("DROP USER IF EXISTS test_user")


def test_drop_detached_with_undrop(start_cluster):
    table_name = "test_drop_detached_with_undrop"

    create_detached_table(replica1, table_name, rows=5)
    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {table_name}"
    )

    assert_eq_with_retry(
        instance=replica1,
        query=dropped_table_count_query(table_name),
        expectation="1",
    )

    replica1.query(f"UNDROP TABLE {table_name}")
    assert "5" == replica1.query(f"SELECT count() FROM {table_name}").rstrip()

    replica1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


def test_drop_detached_with_undrop_after_restart(start_cluster):
    table_name = "test_drop_detached_with_undrop_after_restart"

    create_detached_table(replica1, table_name, rows=7)
    replica1.query(
        f"SET allow_experimental_drop_detached_table=1; "
        f"DROP DETACHED TABLE {table_name}"
    )

    replica1.restart_clickhouse(kill=True)

    assert_eq_with_retry(
        instance=replica1,
        query=dropped_table_count_query(table_name),
        expectation="1",
    )

    replica1.query(f"UNDROP TABLE {table_name}")
    assert "7" == replica1.query(f"SELECT count() FROM {table_name}").rstrip()

    replica1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
