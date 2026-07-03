import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


def fill_nodes(nodes):
    for node in nodes:
        query_with_connect_retry(node, "DROP TABLE IF EXISTS test SYNC")

    for node in nodes:
        query_with_connect_retry(
            node,
            """
            CREATE TABLE test(n UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
            ORDER BY n PARTITION BY n % 10;
        """.format(
                replica=node.name
            ),
        )


cluster = ClickHouseCluster(__file__)
configs = ["configs/remote_servers.xml", "configs/fast_background_pool.xml"]

node_1 = cluster.add_instance("replica1", with_zookeeper=True, main_configs=configs)
node_2 = cluster.add_instance("replica2", with_zookeeper=True, main_configs=configs)
node_3 = cluster.add_instance("replica3", with_zookeeper=True, main_configs=configs)
nodes = [node_1, node_2, node_3]


def fill_table():
    fill_nodes(nodes)
    check_data(0, 0)

    # it will create multiple parts in each partition and probably cause merges
    query_with_connect_retry(node_1, "INSERT INTO test SELECT number + 0 FROM numbers(200)")
    query_with_connect_retry(node_1, "INSERT INTO test SELECT number + 200 FROM numbers(200)")
    query_with_connect_retry(node_1, "INSERT INTO test SELECT number + 400 FROM numbers(200)")
    query_with_connect_retry(node_1, "INSERT INTO test SELECT number + 600 FROM numbers(200)")
    query_with_connect_retry(node_1, "INSERT INTO test SELECT number + 800 FROM numbers(200)")
    check_data(499500, 1000)

def drop_tables():
    for node in nodes:
        query_with_connect_retry(node, "DROP TABLE IF EXISTS test SYNC")


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


# Retry only when clickhouse-client could not open its connection to this node's own
# server (NETWORK_ERROR with "Connection refused (<node ip>:9000)"). In that case the
# query never reached any server, so it is safe to retry even non-idempotent statements.
# A server-side or downstream "Connection refused" (remote shard / Keeper) carries a
# different address and is re-raised immediately, so an already-enqueued ON CLUSTER DDL
# or a partially-applied INSERT is never resubmitted.
def query_with_connect_retry(node, sql, retries=20, sleep_time=0.5, **kwargs):
    connect_refused = f"Connection refused ({node.ip_address}:9000)"
    for attempt in range(retries):
        try:
            return node.query(sql, **kwargs)
        except QueryRuntimeException as ex:
            if (
                ex.returncode == 210
                and connect_refused in str(ex)
                and attempt + 1 < retries
            ):
                print(f"Connection refused from {node.name}, retry {attempt + 1}: {ex}")
                time.sleep(sleep_time)
                continue
            raise


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        fill_nodes(nodes)
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def check_data(_sum: int, count: int) -> None:
    res = "{}\t{}\n".format(_sum, count)
    assert_eq_with_retry(node_1, "SELECT sum(n), count() FROM test", res)
    assert_eq_with_retry(node_2, "SELECT sum(n), count() FROM test", res)
    assert_eq_with_retry(node_3, "SELECT sum(n), count() FROM test", res)


def check_after_restoration():
    check_data(1999000, 2000)

    for node in nodes:
        node.query_and_get_error("SYSTEM RESTORE REPLICA test")


def test_restore_replica_invalid_tables(start_cluster):
    print("Checking the invocation on non-existent and non-replicated tables")
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA i_dont_exist_42")
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA no_db.i_dont_exist_42")
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA system.numbers")


def test_restore_replica_sequential(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    fill_table()

    print("Deleting root ZK path metadata")
    zk_rmr_with_retries(zk, "/clickhouse/tables/test")
    assert zk.exists("/clickhouse/tables/test") is None

    query_with_connect_retry(node_1, "SYSTEM RESTART REPLICA test")
    node_1.query_and_get_error(
        "INSERT INTO test SELECT number AS num FROM numbers(1000,2000) WHERE num % 2 = 0"
    )

    print("Restoring replica1")

    query_with_connect_retry(node_1, "SYSTEM RESTORE REPLICA test")
    assert zk.exists("/clickhouse/tables/test")
    check_data(499500, 1000)

    query_with_connect_retry(node_1, "INSERT INTO test SELECT number + 1000 FROM numbers(1000)")

    print("Restoring other replicas")

    query_with_connect_retry(node_2, "SYSTEM RESTART REPLICA test")
    query_with_connect_retry(node_2, "SYSTEM RESTORE REPLICA test")

    query_with_connect_retry(node_3, "SYSTEM RESTART REPLICA test")
    query_with_connect_retry(node_3, "SYSTEM RESTORE REPLICA test")

    query_with_connect_retry(node_2, "SYSTEM SYNC REPLICA test")
    query_with_connect_retry(node_3, "SYSTEM SYNC REPLICA test")

    check_after_restoration()
    drop_tables()


def test_restore_replica_parallel(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    fill_table()

    print("Deleting root ZK path metadata")
    zk_rmr_with_retries(zk, "/clickhouse/tables/test")
    assert zk.exists("/clickhouse/tables/test") is None

    query_with_connect_retry(node_1, "SYSTEM RESTART REPLICA test")
    node_1.query_and_get_error(
        "INSERT INTO test SELECT number AS num FROM numbers(1000,2000) WHERE num % 2 = 0"
    )

    print("Restoring replicas in parallel")

    query_with_connect_retry(node_2, "SYSTEM RESTART REPLICA test")
    query_with_connect_retry(node_3, "SYSTEM RESTART REPLICA test")

    query_with_connect_retry(node_1, "SYSTEM RESTORE REPLICA test ON CLUSTER test_cluster")

    assert zk.exists("/clickhouse/tables/test")
    check_data(499500, 1000)

    query_with_connect_retry(node_1, "INSERT INTO test SELECT number + 1000 FROM numbers(1000)")

    check_after_restoration()
    drop_tables()


def test_restore_replica_alive_replicas(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    fill_table()

    print("Deleting replica2 path, trying to restore replica1")
    zk_rmr_with_retries(zk, "/clickhouse/tables/test/replicas/replica2")
    assert zk.exists("/clickhouse/tables/test/replicas/replica2") is None
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA test")

    print("Deleting replica1 path, trying to restore replica1")
    zk_rmr_with_retries(zk, "/clickhouse/tables/test/replicas/replica1")
    assert zk.exists("/clickhouse/tables/test/replicas/replica1") is None

    query_with_connect_retry(node_1, "SYSTEM RESTART REPLICA test")
    query_with_connect_retry(node_1, "SYSTEM RESTORE REPLICA test")

    query_with_connect_retry(node_2, "SYSTEM RESTART REPLICA test")
    query_with_connect_retry(node_2, "SYSTEM RESTORE REPLICA test")

    check_data(499500, 1000)

    query_with_connect_retry(node_1, "INSERT INTO test SELECT number + 1000 FROM numbers(1000)")

    query_with_connect_retry(node_2, "SYSTEM SYNC REPLICA test")
    query_with_connect_retry(node_3, "SYSTEM SYNC REPLICA test")

    check_after_restoration()
    drop_tables()


def test_restore_replica_keeps_duplicate_parts(start_cluster):
    zk = cluster.get_kazoo_client("zoo1")
    drop_tables()

    try:
        for node in nodes:
            query_with_connect_retry(
                node,
                """
                CREATE TABLE test(n UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
                ORDER BY tuple()
                SETTINGS replicated_deduplication_window = 0,
                    replicated_deduplication_window_for_async_inserts = 0;
                """.format(
                    replica=node.name
                ),
            )

        query_with_connect_retry(node_1, "SYSTEM STOP MERGES test")

        query_with_connect_retry(node_1, "INSERT INTO test VALUES (0)")
        query_with_connect_retry(node_1, "INSERT INTO test VALUES (0)")
        query_with_connect_retry(
            node_1,
            """
            ALTER TABLE test MODIFY SETTING replicated_deduplication_window = 10000,
                replicated_deduplication_window_for_async_inserts = 10000
            """,
        )

        assert node_1.query("SELECT count() FROM test") == "2\n"
        assert (
            node_1.query("SELECT count() FROM system.parts WHERE table = 'test' AND active")
            == "2\n"
        )

        expected = node_1.query("SELECT count(), sum(sipHash64(*)) FROM test")

        zk_rmr_with_retries(zk, "/clickhouse/tables/test")
        assert zk.exists("/clickhouse/tables/test") is None

        query_with_connect_retry(node_1, "SYSTEM RESTART REPLICA test")
        query_with_connect_retry(node_1, "SYSTEM RESTORE REPLICA test")

        assert node_1.query("SELECT count() FROM test") == "2\n"
        assert node_1.query("SELECT count(), sum(sipHash64(*)) FROM test") == expected
    finally:
        drop_tables()
