import time

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseKiller
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
configs = ["configs/remote_servers.xml"]

node_1 = cluster.add_instance(
    name="node1",
    main_configs=configs,
    with_zookeeper=True,
    macros={"replica": "replica1", "shard": "shard1"},
    stay_alive=True,
)
node_2 = cluster.add_instance(
    name="node2",
    main_configs=configs,
    macros={"replica": "replica2", "shard": "shard1"},
    with_zookeeper=True,
)
cluster_nodes = [node_1, node_2]


def prepare_dbs():
    for node in cluster_nodes:
        node.query("DROP DATABASE IF EXISTS repl_db SYNC")
        node.query(
            """
                CREATE DATABASE repl_db 
                ENGINE=Replicated("/clickhouse/repl_db", '{shard}', '{replica}')
            """
        )


def create_table(node, table_name: str):
    assert node.query(
        f"""
            CREATE TABLE repl_db.{table_name} (n UInt32)
            ENGINE = ReplicatedMergeTree
            ORDER BY n PARTITION BY n % 10;
        """
    )


def fill_table(node, table_name: str, amount: int):
    node.query(
        f"""
            INSERT INTO repl_db.{table_name} SELECT number FROM numbers({amount})
        """
    )


def check_contains_table(node, table_name: str, amount: int):
    assert [f"{amount}"] == node.query(f"SELECT count(*) FROM repl_db.{table_name}").split()


def failed_create_table(node, table_name: str):
    assert node.query_and_get_error(
        f"""
            CREATE TABLE repl_db.{table_name} (n UInt32)
            ENGINE = ReplicatedMergeTree
            ORDER BY n PARTITION BY n % 10;
        """
    )


def get_tables_from_replicated(node):
    return node.query("SELECT table FROM system.tables WHERE database='repl_db' ORDER BY table").split()


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
        prepare_dbs()
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "need_restart",
    [
        pytest.param(
            False,
            id="no restart",
        ),
        pytest.param(
            True,
            id="with restart",
        ),
    ]
)
@pytest.mark.parametrize(
    "exists_table",
    [
        pytest.param(
            None,
            id="no exists table",
        ),
        pytest.param(
            "exists_table",
            id="with exists table",
        ),
    ]
)
@pytest.mark.parametrize(
    "need_fill_tables",
    [
        pytest.param(
            False,
            id="empty tables",
        ),
        pytest.param(
            True,
            id="fill tables",
        ),
    ]
)
@pytest.mark.parametrize(
    "new_table",
    [
        pytest.param(
            "test_create_table",
            id="create table",
        ),
        # drop
        # rename
        # alter
    ]
)
def test_query_after_restore_db_replica(start_cluster, need_restart, exists_table, new_table, need_fill_tables):
    inserted_data = 1000

    if exists_table:
        create_table(node_1, exists_table)

        if need_fill_tables:
            fill_table(node_1, exists_table, inserted_data)

    zk = cluster.get_kazoo_client("zoo1")

    zk_rmr_with_retries(zk, "/clickhouse/repl_db")
    assert zk.exists("/clickhouse/repl_db") is None

    expected_tables = []

    if exists_table:
        expected_tables.append(exists_table)

    assert expected_tables == get_tables_from_replicated(node_1)
    assert expected_tables == get_tables_from_replicated(node_2)
        
    failed_create_table(node_1, new_table)

    assert expected_tables == get_tables_from_replicated(node_1)
    assert expected_tables == get_tables_from_replicated(node_2)

    if need_restart:
        node_1.restart_clickhouse()

    assert zk.exists(f"/clickhouse/repl_db/metadata/{exists_table}") is None
    assert zk.exists(f"/clickhouse/repl_db/metadata/{new_table}") is None

    node_1.query("SYSTEM RESTORE DATABASE REPLICA repl_db")

    if exists_table:
        assert zk.exists(f"/clickhouse/repl_db/metadata/{exists_table}")
        if need_fill_tables:
            check_contains_table(node_1, exists_table, inserted_data)

    assert zk.exists(f"/clickhouse/repl_db/metadata/{new_table}") is None

    assert zk.exists("/clickhouse/repl_db/replicas/shard1|replica1")
    assert zk.exists("/clickhouse/repl_db/replicas/shard1|replica2") is None

    node_2.query("SYSTEM RESTORE DATABASE REPLICA repl_db")
    assert zk.exists("/clickhouse/repl_db/replicas/shard1|replica2")

    if exists_table:
        assert [exists_table] == get_tables_from_replicated(node_1)
        assert [exists_table] == get_tables_from_replicated(node_2)
        if need_fill_tables:
            check_contains_table(node_1, exists_table, inserted_data)
            check_contains_table(node_2, exists_table, inserted_data)

    create_table(node_1, new_table)
    if need_fill_tables:
        fill_table(node_1, new_table, inserted_data)

    expected_tables = [new_table]
    if exists_table:
        expected_tables.append(exists_table)

    expected_tables.sort()

    assert expected_tables == get_tables_from_replicated(node_1)
    assert expected_tables == get_tables_from_replicated(node_2)

    if need_fill_tables:
        check_contains_table(node_1, new_table, inserted_data)
        check_contains_table(node_2, new_table, inserted_data)

    if exists_table:
        assert zk.exists(f"/clickhouse/repl_db/metadata/{exists_table}")

    assert zk.exists(f"/clickhouse/repl_db/metadata/{new_table}")

    if exists_table:
        node_1.query(f"DROP TABLE repl_db.{exists_table} SYNC")
    node_1.query(f"DROP TABLE repl_db.{new_table} SYNC")
