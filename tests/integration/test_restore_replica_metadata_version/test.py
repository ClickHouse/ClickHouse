import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
configs = ["configs/fast_background_pool.xml"]

node_1 = cluster.add_instance("replica1", with_zookeeper=True, main_configs=configs)
node_2 = cluster.add_instance("replica2", with_zookeeper=True, main_configs=configs)


# kazoo.delete may throw NotEmptyError on concurrent modifications of the path
def zk_rmr_with_retries(zk, path):
    for i in range(1, 10):
        try:
            zk.delete(path, recursive=True)
            return
        except Exception as ex:
            print(ex)
            time.sleep(0.5)
    assert False


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def test_fix_metadata_version_on_attach_part_after_restore(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")

    for node in [node_1, node_2]:
        node.query("DROP TABLE IF EXISTS test_ttl SYNC")

    for node in [node_1, node_2]:
        node.query(
            """
            CREATE TABLE test_ttl(n UInt32, d DateTime)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_ttl/', '{replica}')
            TTL d + INTERVAL 5 SECOND DELETE
            ORDER BY n PARTITION BY n % 10
            SETTINGS merge_with_ttl_timeout = 0, number_of_free_entries_in_pool_to_execute_mutation = 0;
        """.format(
                replica=node.name
            )
        )

    # Alter metadata to increment metadata version of the table
    node_1.query("ALTER TABLE test_ttl ADD COLUMN Added1 UInt32")
    node_1.query("ALTER TABLE test_ttl DROP COLUMN Added1")

    node_1.query("SYSTEM STOP TTL MERGES test_ttl")

    # Create a part
    node_1.query("INSERT INTO test_ttl VALUES (1, now())")

    # Delete root zk metadata path for the table
    zk_rmr_with_retries(zk, "/clickhouse/tables/test_ttl")

    # Restore replicas
    node_1.query("SYSTEM RESTART REPLICA test_ttl")
    node_1.query("SYSTEM RESTORE REPLICA test_ttl")
    # Disable TTL merges on node_1. We expect it to be done on node_2
    node_1.query("SYSTEM STOP TTL MERGES test_ttl")

    node_2.query("SYSTEM RESTART REPLICA test_ttl")
    node_2.query("SYSTEM RESTORE REPLICA test_ttl")

    # TTL merge should work.
    # Before fix it is failed due to metadata version discrepancy and we will have in the log:
    # "Source part metadata version 2 is newer then the table metadata version 0. ALTER_METADATA is still in progress"
    assert_eq_with_retry(
        node_2, "SELECT count() FROM test_ttl", "0\n", retry_count=60, sleep_time=1
    )

    for node in [node_1, node_2]:
        node.query("DROP TABLE IF EXISTS test_ttl SYNC")


def test_alter_after_restoring_every_replica(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")

    for node in [node_1, node_2]:
        node.query("DROP TABLE IF EXISTS test_restored_alter SYNC")

    for node in [node_1, node_2]:
        node.query(
            """
            CREATE TABLE test_restored_alter(n UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_restored_alter/', '{replica}')
            ORDER BY n
        """.format(
                replica=node.name
            )
        )

    # Two metadata ALTERs bring the table's metadata version to 2 on both replicas
    node_1.query(
        "ALTER TABLE test_restored_alter ADD COLUMN Added1 UInt32",
        settings={"alter_sync": 2},
    )
    node_1.query(
        "ALTER TABLE test_restored_alter DROP COLUMN Added1",
        settings={"alter_sync": 2},
    )
    node_1.query("INSERT INTO test_restored_alter VALUES (1)")
    node_2.query("SYSTEM SYNC REPLICA test_restored_alter")

    zk_rmr_with_retries(zk, "/clickhouse/tables/test_restored_alter")

    for node in [node_1, node_2]:
        node.query("SYSTEM RESTART REPLICA test_restored_alter")
        node.query("SYSTEM RESTORE REPLICA test_restored_alter")

    # Restoring the second replica used to bump the version a second time, to 4
    _, stat = zk.get("/clickhouse/tables/test_restored_alter/metadata")
    assert stat.version == 2

    # after which every ALTER failed with CANNOT_ASSIGN_ALTER
    node_1.query(
        "ALTER TABLE test_restored_alter ADD COLUMN Added2 UInt32",
        settings={"alter_sync": 2},
    )
    for node in [node_1, node_2]:
        assert node.query("SELECT n, Added2 FROM test_restored_alter") == "1\t0\n"

    for node in [node_1, node_2]:
        node.query("DROP TABLE IF EXISTS test_restored_alter SYNC")


def test_alter_after_restoring_lagging_replica_first(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    table = "test_restored_lagging_first"

    for node in [node_1, node_2]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    for node in [node_1, node_2]:
        node.query(
            f"CREATE TABLE {table}(n UInt32) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}/', '{node.name}') ORDER BY n"
        )

    node_1.query(
        f"ALTER TABLE {table} ADD COLUMN a1 UInt32", settings={"alter_sync": 2}
    )
    node_1.query(f"INSERT INTO {table} (n) VALUES (1)")
    node_2.query(f"SYSTEM SYNC REPLICA {table}")
    node_1.query(
        f"ALTER TABLE {table} ADD COLUMN a2 UInt32", settings={"alter_sync": 2}
    )

    # node_2 lags: its newest part is at metadata version 1, node_1 has one at version 2
    node_2.query(f"SYSTEM STOP FETCHES {table}")
    node_1.query(f"INSERT INTO {table} (n) VALUES (2)")

    zk_rmr_with_retries(zk, f"/clickhouse/tables/{table}")

    for node in [node_2, node_1]:
        node.query(f"SYSTEM START FETCHES {table}")
        node.query(f"SYSTEM RESTART REPLICA {table}")
        node.query(f"SYSTEM RESTORE REPLICA {table}")
    for node in [node_1, node_2]:
        node.query(f"SYSTEM SYNC REPLICA {table}")

    _, stat = zk.get(f"/clickhouse/tables/{table}/metadata")
    for node in [node_1, node_2]:
        version, _ = zk.get(
            f"/clickhouse/tables/{table}/replicas/{node.name}/metadata_version"
        )
        assert int(version) == stat.version

    for i, node in enumerate([node_2, node_1]):
        node.query(
            f"ALTER TABLE {table} ADD COLUMN b{i} UInt32", settings={"alter_sync": 2}
        )

    for node in [node_1, node_2]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_alter_after_restoring_replicas_concurrently(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    table = "test_restored_concurrently"
    failpoint = "rmt_restore_replica_pause_during_metadata_bump"

    for node in [node_1, node_2]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    for node in [node_1, node_2]:
        node.query(
            f"CREATE TABLE {table}(n UInt32) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}/', '{node.name}') ORDER BY n"
        )

    # Two metadata ALTERs bring the table's metadata version to 2 on both replicas
    node_1.query(
        f"ALTER TABLE {table} ADD COLUMN a1 UInt32", settings={"alter_sync": 2}
    )
    node_1.query(f"ALTER TABLE {table} DROP COLUMN a1", settings={"alter_sync": 2})
    node_1.query(f"INSERT INTO {table} VALUES (1)")
    node_2.query(f"SYSTEM SYNC REPLICA {table}")

    zk_rmr_with_retries(zk, f"/clickhouse/tables/{table}")
    for node in [node_1, node_2]:
        node.query(f"SYSTEM RESTART REPLICA {table}")

    # Pause the first restore with the shared version at 1 of 2, and let the second replica
    # restore and clone the first one meanwhile, as concurrent restores (ON CLUSTER) can
    node_1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    first_restore = node_1.get_query_request(f"SYSTEM RESTORE REPLICA {table}")
    node_1.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE")
    node_2.query(f"SYSTEM RESTORE REPLICA {table}")
    assert_eq_with_retry(
        node_2,
        f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'",
        "0\n",
    )
    node_1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
    first_restore.get_answer()

    for node in [node_1, node_2]:
        node.query(f"SYSTEM SYNC REPLICA {table}")

    # The second replica used to keep the intermediate version 1 and fail every ALTER
    _, stat = zk.get(f"/clickhouse/tables/{table}/metadata")
    assert stat.version == 2
    for node in [node_1, node_2]:
        version, _ = zk.get(
            f"/clickhouse/tables/{table}/replicas/{node.name}/metadata_version"
        )
        assert int(version) == stat.version

    for i, node in enumerate([node_1, node_2]):
        node.query(
            f"ALTER TABLE {table} ADD COLUMN b{i} UInt32", settings={"alter_sync": 2}
        )
    for node in [node_1, node_2]:
        assert node.query(f"SELECT n, b0, b1 FROM {table}") == "1\t0\t0\n"

    for node in [node_1, node_2]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
