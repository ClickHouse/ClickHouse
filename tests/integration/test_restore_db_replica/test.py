import time

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseKiller


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


def failed_create_table(node, table_name: str):
    assert node.query_and_get_error(
        f"""
            CREATE TABLE repl_db.{table_name} (n UInt32)
            ENGINE = ReplicatedMergeTree
            ORDER BY n PARTITION BY n % 10;
        """
    )


def failed_rename_table(node, table_name: str, new_table_name: str):
    assert node.query_and_get_error(
        f"""
            RENAME TABLE repl_db.{table_name} TO repl_db.{new_table_name}
        """
    )


def failed_alter_table(node, table_name: str):
    assert node.query_and_get_error(
        f"""
            ALTER TABLE repl_db.{table_name} ADD COLUMN m String
        """
    )


def create_table(node, table_name: str):
    assert node.query(
        f"""
            SET distributed_ddl_task_timeout=10;
            CREATE TABLE repl_db.{table_name} (n UInt32)
            ENGINE = ReplicatedMergeTree
            ORDER BY n PARTITION BY n % 10;
        """
    )


def rename_table(node, table_name: str, new_table_name: str):
    assert node.query(
        f"""
            RENAME TABLE repl_db.{table_name} TO repl_db.{new_table_name}
        """
    )


def alter_table(node, table_name: str):
    assert node.query(
        f"""
            ALTER TABLE repl_db.{table_name} ADD COLUMN m String
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
    "exists_table, handler_create_table",
    [
        pytest.param(
            None,
            None,
            id="no exists table",
        ),
        pytest.param(
            "exists_table",
            create_table,
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
    "process_table, failed_action_with_table, action_with_table",
    [
        pytest.param(
            "test_create_table",
            failed_create_table,
            create_table,
            id="create table",
        ),
    ]
)
@pytest.mark.parametrize(
    "changed_table, failed_change_table, change_table",
    [
        pytest.param(
            "renamed_table",
            lambda node, t1, t2: failed_rename_table(node, t1, t2),
            lambda node, t1, t2: rename_table(node, t1, t2),
            id="rename table",
        ),
        pytest.param(
            "test_create_table",
            lambda node, t1, _: failed_alter_table(node, t1),
            lambda node, t1, _: alter_table(node, t1),
            id="alter table",
        ),
    ]
)
def test_query_after_restore_db_replica(
    start_cluster, 
    need_restart, 
    exists_table, 
    handler_create_table,
    need_fill_tables,
    process_table,
    failed_action_with_table,
    action_with_table,
    changed_table,
    failed_change_table,
    change_table
    ):
    
    inserted_data = 1000

    if exists_table:
        handler_create_table(node_1, exists_table)

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
        
    failed_action_with_table(node_1, process_table)

    assert expected_tables == get_tables_from_replicated(node_1)
    assert expected_tables == get_tables_from_replicated(node_2)

    if need_restart:
        node_1.restart_clickhouse()

    assert zk.exists(f"/clickhouse/repl_db/metadata/{exists_table}") is None
    assert zk.exists(f"/clickhouse/repl_db/metadata/{process_table}") is None

    node_1.query("SYSTEM RESTORE DATABASE REPLICA repl_db")

    if exists_table:
        assert zk.exists(f"/clickhouse/repl_db/metadata/{exists_table}")
        if need_fill_tables:
            check_contains_table(node_1, exists_table, inserted_data)

    assert zk.exists(f"/clickhouse/repl_db/metadata/{process_table}") is None

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

    action_with_table(node_1, process_table)
    if need_fill_tables:
        fill_table(node_1, process_table, inserted_data)

    expected_tables = [process_table]
    if exists_table:
        expected_tables.append(exists_table)

    expected_tables.sort()

    assert expected_tables == get_tables_from_replicated(node_1)
    assert expected_tables == get_tables_from_replicated(node_2)

    if need_fill_tables:
        check_contains_table(node_1, process_table, inserted_data)
        check_contains_table(node_2, process_table, inserted_data)

    if exists_table:
        assert zk.exists(f"/clickhouse/repl_db/metadata/{exists_table}")

    assert zk.exists(f"/clickhouse/repl_db/metadata/{process_table}")

    zk_rmr_with_retries(zk, "/clickhouse/repl_db")
    assert zk.exists("/clickhouse/repl_db") is None

    failed_change_table(node_1, process_table, changed_table)

    if need_restart:
        node_1.restart_clickhouse()

    assert zk.exists(f"/clickhouse/repl_db/metadata/{exists_table}") is None
    assert zk.exists(f"/clickhouse/repl_db/metadata/{process_table}") is None

    node_1.query("SYSTEM RESTORE DATABASE REPLICA repl_db")
    node_2.query("SYSTEM RESTORE DATABASE REPLICA repl_db")

    if exists_table:
        assert zk.exists(f"/clickhouse/repl_db/metadata/{exists_table}")
    assert zk.exists(f"/clickhouse/repl_db/metadata/{process_table}")

    change_table(node_1, process_table, changed_table)

    if process_table != changed_table:
        assert zk.exists(f"/clickhouse/repl_db/metadata/{process_table}") is None
    assert zk.exists(f"/clickhouse/repl_db/metadata/{changed_table}")

    expected_tables = [changed_table]
    if exists_table:
        expected_tables.append(exists_table)
    expected_tables.sort()

    assert expected_tables == get_tables_from_replicated(node_1)
    assert expected_tables == get_tables_from_replicated(node_2)

    if need_fill_tables:
        if exists_table:
            check_contains_table(node_1, exists_table, inserted_data)
        check_contains_table(node_2, changed_table, inserted_data)

    if exists_table:
        node_1.query(f"DROP TABLE repl_db.{exists_table} SYNC")
    node_1.query(f"DROP TABLE repl_db.{changed_table} SYNC")


@pytest.mark.parametrize(
    "restore_firstly_node_where_created",
    [
        pytest.param(
            [node_1, node_2],
            id="restore node1-node2",
        ),
        pytest.param(
            True,
            id="restore node2-node1",
        ),
    ]
)
def test_restore_db_replica_with_diffrent_table_metadata(
    start_cluster,
    restore_firstly_node_where_created
):
    test_table_1 = "test_table_1"
    test_table_2 = "test_table_2"
    
    node_1.query(f"DROP TABLE IF EXISTS repl_db.{test_table_1} SYNC")
    node_2.query(f"DROP TABLE IF EXISTS repl_db.{test_table_1} SYNC")
    node_1.query(f"DROP TABLE IF EXISTS repl_db.{test_table_2} SYNC")
    node_2.query(f"DROP TABLE IF EXISTS repl_db.{test_table_2} SYNC")

    zk = cluster.get_kazoo_client("zoo1")

    count_test_table_1 = 100

    create_table(node_1, test_table_1)
    fill_table(node_1, test_table_1, count_test_table_1)

    node_1.stop_clickhouse()

    assert "is not finished on 1 of 2 hosts" in node_2.query_and_get_error(
        f"""
            SET distributed_ddl_task_timeout=10;
            CREATE TABLE repl_db.{test_table_2} (n UInt32)
            ENGINE = ReplicatedMergeTree
            ORDER BY n PARTITION BY n % 10;
        """
    )

    count_test_table_2 = 10

    fill_table(node_2, test_table_2, count_test_table_2)

    zk_rmr_with_retries(zk, "/clickhouse/repl_db")
    assert zk.exists("/clickhouse/repl_db") is None

    node_1.start_clickhouse()

    assert ["0"] == node_1.query(f"SELECT count(*) FROM system.tables WHERE table='{test_table_2}'").split()
    assert ["1"] == node_2.query(f"SELECT count(*) FROM system.tables WHERE table='{test_table_2}'").split()

    nodes = [node_1, node_2]
    if restore_firstly_node_where_created:
        nodes.reverse()

    for node in nodes:
        node.query("SYSTEM RESTORE DATABASE REPLICA repl_db")

    assert [f"{count_test_table_1}"] == node_1.query(f"SELECT count(*) FROM repl_db.{test_table_1}").split()
    assert [f"{count_test_table_1}"] == node_2.query(f"SELECT count(*) FROM repl_db.{test_table_1}").split()

    expected_count = ["0"]
    if restore_firstly_node_where_created:
        expected_count = ["1"]

    assert expected_count == node_1.query(f"SELECT count(*) FROM system.tables WHERE table='{test_table_2}'").split()
    assert expected_count == node_2.query(f"SELECT count(*) FROM system.tables WHERE table='{test_table_2}'").split()

    if restore_firstly_node_where_created:
        assert [f"{count_test_table_2}"] == node_1.query(f"SELECT count(*) FROM repl_db.{test_table_2}").split()
        assert [f"{count_test_table_2}"] == node_2.query(f"SELECT count(*) FROM repl_db.{test_table_2}").split()
