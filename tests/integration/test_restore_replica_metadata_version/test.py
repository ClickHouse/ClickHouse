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
