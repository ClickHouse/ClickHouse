import random
import string
import time

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseKiller
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
configs = ["configs/remote_servers.xml", "configs/logger.xml"]

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


@pytest.fixture(scope="function")
def test_name(request):
    return request.node.name


def generate_random_string(length=6):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


@pytest.fixture(scope="function")
def exclusive_database_name(test_name):
    normalized = (
        test_name.replace("[", "_")
        .replace("]", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )
    return "repl_db_" + normalized + "_" + generate_random_string()


def prepare_db(db_name: str):
    for node in cluster_nodes:
        node.query(
            f"""
                CREATE DATABASE {db_name}
                ENGINE=Replicated("/clickhouse/{db_name}", \'{{shard}}\', \'{{replica}}\')
            """
        )


def failed_create_table(node, table_name: str):
    node.query_and_get_error(
        f"""
            CREATE TABLE {table_name} (n UInt32)
            ENGINE = ReplicatedMergeTree
            ORDER BY n PARTITION BY n % 10;
        """
    )


def failed_rename_table(node, table_name: str, new_table_name: str):
    node.query_and_get_error(
        f"""
            RENAME TABLE {table_name} TO {new_table_name}
        """
    )


def failed_alter_table(node, table_name: str):
    node.query_and_get_error(
        f"""
            ALTER TABLE {table_name} ADD COLUMN m String
        """
    )


def create_table(node, table_name: str):
    node.query(
        f"""
            CREATE TABLE {table_name} (n UInt32)
            ENGINE = ReplicatedMergeTree
            ORDER BY n PARTITION BY n % 10
            SETTINGS distributed_ddl_task_timeout=5;
        """
    )


def rename_table(node, table_name: str, new_table_name: str):
    node.query(
        f"""
            RENAME TABLE {table_name} TO {new_table_name}
        """
    )


def alter_table(node, table_name: str):
    node.query(
        f"""
            ALTER TABLE {table_name} ADD COLUMN m String
        """
    )


def fill_table(node, table_name: str, amount: int):
    node.query(
        f"""
            INSERT INTO {table_name} SELECT number FROM numbers({amount})
        """
    )


def check_contains_table(node, table_name: str, amount: int):
    node.query(f"SYSTEM SYNC REPLICA {table_name}")
    assert [f"{amount}"] == node.query(f"SELECT count(*) FROM {table_name}").split()


def get_tables_from_replicated(node, db_name: str):
    return node.query(
        f"SELECT table FROM system.tables WHERE database='{db_name}' ORDER BY table"
    ).split()


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


def count_log_message(node, db_name, msg):
    node.query("SYSTEM FLUSH LOGS")
    return int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE logger_name='DatabaseReplicated ({db_name})' AND message ='{msg}'"
        ).strip()
    )


def restore_database_and_wait(node, db_name: str, on_cluster):
    print(f"Restore database node {node.name}, db {db_name}")
    log_msg = "All tables are created successfully"
    prev_count = count_log_message(node, db_name, log_msg)

    if on_cluster is not None:
        node.query(
            f"SYSTEM RESTORE DATABASE REPLICA ON CLUSTER `{on_cluster}` `{db_name}`"
        )
    else:
        node.query(f"SYSTEM RESTORE DATABASE REPLICA `{db_name}`")

    for i in range(30):
        current_count = count_log_message(node, db_name, log_msg)
        if current_count > prev_count:
            return
        time.sleep(0.5)

    raise Exception(f"Creating all table timed out when restoring database {db_name}")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
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
    ],
)
@pytest.mark.parametrize(
    "exists_table, handler_create_table",
    [
        pytest.param(
            False,
            None,
            id="no exists table",
        ),
        pytest.param(
            True,
            create_table,
            id="with exists table",
        ),
    ],
)
@pytest.mark.parametrize(
    "changed_table, failed_change_table, change_table",
    [
        pytest.param(
            "test_rename_table",
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
    ],
)
def test_query_after_restore_db_replica(
    start_cluster,
    exclusive_database_name,
    need_restart,
    exists_table,
    handler_create_table,
    changed_table,
    failed_change_table,
    change_table,
):
    process_table = "test_create_table"

    prepare_db(exclusive_database_name)
    inserted_data = 1000

    exists_table_name = "exists_table_" + generate_random_string()

    if exists_table:
        handler_create_table(node_1, f"{exclusive_database_name}.{exists_table_name}")

        fill_table(
            node_1, f"{exclusive_database_name}.{exists_table_name}", inserted_data
        )

    zk = cluster.get_kazoo_client("zoo1")

    zk_rmr_with_retries(zk, f"/clickhouse/{exclusive_database_name}")
    assert zk.exists(f"/clickhouse/{exclusive_database_name}") is None

    expected_tables = []

    if exists_table:
        expected_tables.append(exists_table_name)

    assert expected_tables == get_tables_from_replicated(
        node_1, exclusive_database_name
    )
    assert expected_tables == get_tables_from_replicated(
        node_2, exclusive_database_name
    )

    failed_create_table(node_1, f"{exclusive_database_name}.{process_table}")

    assert expected_tables == get_tables_from_replicated(
        node_1, exclusive_database_name
    )
    assert expected_tables == get_tables_from_replicated(
        node_2, exclusive_database_name
    )

    assert (
        zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{exists_table_name}")
        is None
    )
    assert (
        zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{process_table}")
        is None
    )

    restore_database_and_wait(node_1, exclusive_database_name, None)

    if need_restart:
        node_1.restart_clickhouse()

    assert (
        zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{process_table}")
        is None
    )

    assert zk.exists(f"/clickhouse/{exclusive_database_name}/replicas/shard1|replica1")
    assert (
        zk.exists(f"/clickhouse/{exclusive_database_name}/replicas/shard1|replica2")
        is None
    )

    restore_database_and_wait(node_2, exclusive_database_name, None)
    assert zk.exists(f"/clickhouse/{exclusive_database_name}/replicas/shard1|replica2")

    if exists_table:
        assert node_1.query_with_retry(
            f"SELECT table FROM system.tables WHERE database='{exclusive_database_name}' ORDER BY table",
            retry_count=30,
            sleep_time=1,
            check_callback=lambda tables: tables.strip() == exists_table_name,
        )
        assert node_2.query_with_retry(
            f"SELECT table FROM system.tables WHERE database='{exclusive_database_name}' ORDER BY table",
            retry_count=30,
            sleep_time=1,
            check_callback=lambda tables: tables.strip() == exists_table_name,
        )
        check_contains_table(
            node_1, f"{exclusive_database_name}.{exists_table_name}", inserted_data
        )
        check_contains_table(
            node_2, f"{exclusive_database_name}.{exists_table_name}", inserted_data
        )

    create_table(node_1, f"{exclusive_database_name}.{process_table}")
    fill_table(node_1, f"{exclusive_database_name}.{process_table}", inserted_data)

    expected_tables = [process_table]
    if exists_table:
        expected_tables.append(exists_table_name)

    expected_tables.sort()

    assert expected_tables == get_tables_from_replicated(
        node_1, exclusive_database_name
    )
    assert expected_tables == get_tables_from_replicated(
        node_2, exclusive_database_name
    )

    check_contains_table(
        node_1, f"{exclusive_database_name}.{process_table}", inserted_data
    )
    check_contains_table(
        node_2, f"{exclusive_database_name}.{process_table}", inserted_data
    )

    if exists_table:
        assert zk.exists(
            f"/clickhouse/{exclusive_database_name}/metadata/{exists_table_name}"
        )

    assert zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{process_table}")

    zk_rmr_with_retries(zk, f"/clickhouse/{exclusive_database_name}")
    assert zk.exists(f"/clickhouse/{exclusive_database_name}") is None

    failed_change_table(
        node_1,
        f"{exclusive_database_name}.{process_table}",
        f"{exclusive_database_name}.{changed_table}",
    )

    assert (
        zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{exists_table_name}")
        is None
    )
    assert (
        zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{process_table}")
        is None
    )

    restore_database_and_wait(node_1, exclusive_database_name, None)
    restore_database_and_wait(node_2, exclusive_database_name, None)

    if need_restart:
        node_1.restart_clickhouse()

    if exists_table:
        assert zk.exists(
            f"/clickhouse/{exclusive_database_name}/metadata/{exists_table_name}"
        )
    assert zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{process_table}")

    change_table(
        node_1,
        f"{exclusive_database_name}.{process_table}",
        f"{exclusive_database_name}.{changed_table}",
    )

    if process_table != changed_table:
        assert (
            zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{process_table}")
            is None
        )
    assert zk.exists(f"/clickhouse/{exclusive_database_name}/metadata/{changed_table}")

    expected_tables = [changed_table]
    if exists_table:
        expected_tables.append(exists_table_name)
    expected_tables.sort()

    assert expected_tables == get_tables_from_replicated(
        node_1, exclusive_database_name
    )
    assert expected_tables == get_tables_from_replicated(
        node_2, exclusive_database_name
    )

    if exists_table:
        check_contains_table(
            node_1, f"{exclusive_database_name}.{exists_table_name}", inserted_data
        )
    check_contains_table(
        node_2, f"{exclusive_database_name}.{changed_table}", inserted_data
    )

    if exists_table:
        node_1.query(f"DROP TABLE {exclusive_database_name}.{exists_table_name} SYNC")
    node_1.query(f"DROP TABLE {exclusive_database_name}.{changed_table} SYNC")

    assert node_1.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}'"
    ) == TSV([0]) 
    assert node_2.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}'"
    ) == TSV([0]) 

    node_1.query(f"DROP DATABASE {exclusive_database_name} SYNC")
    node_2.query(f"DROP DATABASE {exclusive_database_name} SYNC")

    node_1.query(
        f"DROP DATABASE IF EXISTS {exclusive_database_name}_broken_tables SYNC"
    )
    node_2.query(
        f"DROP DATABASE IF EXISTS {exclusive_database_name}_broken_tables SYNC"
    )

    node_1.query(
        f"DROP DATABASE IF EXISTS {exclusive_database_name}_broken_replicated_tables SYNC"
    )
    node_2.query(
        f"DROP DATABASE IF EXISTS {exclusive_database_name}_broken_replicated_tables SYNC"
    )


@pytest.mark.parametrize(
    "restore_firstly_node_where_created",
    [
        pytest.param(
            False,
            id="restore node1-node2",
        ),
        pytest.param(
            True,
            id="restore node2-node1",
        ),
    ],
)
def test_restore_db_replica_with_diffrent_table_metadata(
    start_cluster, exclusive_database_name, restore_firstly_node_where_created
):
    prepare_db(exclusive_database_name)

    test_table_1 = "test_table_1"
    test_table_2 = "test_table_2"

    zk = cluster.get_kazoo_client("zoo1")

    count_test_table_1 = 100

    create_table(node_1, f"{exclusive_database_name}.{test_table_1}")
    fill_table(node_1, f"{exclusive_database_name}.{test_table_1}", count_test_table_1)

    node_1.stop_clickhouse()

    assert "is not finished on 1 of 2 hosts" in node_2.query_and_get_error(
        f"""
            SET distributed_ddl_task_timeout=10;
            CREATE TABLE {exclusive_database_name}.{test_table_2} (n UInt32)
            ENGINE = ReplicatedMergeTree
            ORDER BY n PARTITION BY n % 10;
        """
    )

    count_test_table_2 = 10

    fill_table(node_2, f"{exclusive_database_name}.{test_table_2}", count_test_table_2)

    zk_rmr_with_retries(zk, f"/clickhouse/{exclusive_database_name}")
    assert zk.exists(f"/clickhouse/{exclusive_database_name}") is None

    node_1.start_clickhouse()

    assert node_1.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}' AND table='{test_table_2}'"
    ) == TSV([0]) 
    assert node_2.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}' AND table='{test_table_2}'"
    ) == TSV([1]) 

    nodes = [node_1, node_2]
    if restore_firstly_node_where_created:
        nodes.reverse()

    for node in nodes:
        restore_database_and_wait(node, exclusive_database_name, None)

    assert node_1.query_with_retry(
        f"SELECT count(*) FROM {exclusive_database_name}.{test_table_1}",
        check_callback=lambda x: x.strip() == str(count_test_table_1),
    ) == TSV([count_test_table_1])
    assert node_2.query_with_retry(
        f"SELECT count(*) FROM {exclusive_database_name}.{test_table_1}",
        check_callback=lambda x: x.strip() == str(count_test_table_1),
    ) == TSV([count_test_table_1])

    expected_count = ["0"]
    if restore_firstly_node_where_created:
        expected_count = ["1"]

    assert (
        node_1.query(
            f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}' AND table='{test_table_2}'"
        ) == TSV([expected_count]) 
    )
    assert (
        node_2.query(
            f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}' AND table='{test_table_2}'"
        ) == TSV([expected_count]) 
    )

    if restore_firstly_node_where_created:
        check_contains_table(
            node_1, f"{exclusive_database_name}.{test_table_2}", count_test_table_2
        )
        check_contains_table(
            node_2, f"{exclusive_database_name}.{test_table_2}", count_test_table_2
        )
    else:
        assert node_2.query(
            f"SELECT count(*) FROM system.databases WHERE name='{exclusive_database_name}_broken_tables'"
        )  == TSV([1]) 
        assert node_2.query(
            f"SELECT count(*) FROM system.databases WHERE name='{exclusive_database_name}_broken_replicated_tables'"
        ) == TSV([1]) 
        assert (
            node_2.query(
                f"SELECT table FROM system.tables WHERE database='{exclusive_database_name}_broken_tables'"
            ) == TSV([]) 
        )

        detached_broken_tables = node_2.query(
            f"SELECT table FROM system.tables WHERE database='{exclusive_database_name}_broken_replicated_tables'"
        ).split()

        assert len(detached_broken_tables) == 1
        assert detached_broken_tables[0].startswith(f"{test_table_2}_")

        assert node_2.query(
            f"SELECT count(*) FROM {exclusive_database_name}_broken_replicated_tables.{detached_broken_tables[0]}"
        ) == TSV([count_test_table_2]) 

        node_2.query(f"DROP DATABASE {exclusive_database_name}_broken_tables SYNC")
        node_2.query(
            f"DROP DATABASE {exclusive_database_name}_broken_replicated_tables SYNC"
        )

    node_1.query(f"DROP TABLE IF EXISTS {exclusive_database_name}.{test_table_1} SYNC")
    node_1.query(f"DROP TABLE IF EXISTS {exclusive_database_name}.{test_table_2} SYNC")

    assert node_1.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}'"
    ) == TSV([0]) 
    assert node_2.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}'"
    ) == TSV([0]) 

    node_1.query(f"DROP DATABASE {exclusive_database_name} SYNC")
    node_2.query(f"DROP DATABASE {exclusive_database_name} SYNC")


def test_failed_restore_db_replica_on_normal_replica(
    start_cluster,
    exclusive_database_name,
):
    prepare_db(exclusive_database_name)

    test_table = "test_table_normal_replica"

    count_test_table = 100

    create_table(node_1, f"{exclusive_database_name}.{test_table}")
    fill_table(node_1, f"{exclusive_database_name}.{test_table}", count_test_table)

    assert (
        f"Replica node '/clickhouse/{exclusive_database_name}/replicas/shard1|replica1/digest' in ZooKeeper already exists"
        in node_1.query_and_get_error(
            f"SYSTEM RESTORE DATABASE REPLICA {exclusive_database_name}"
        )
    )

    assert (
        f"Replica node '/clickhouse/{exclusive_database_name}/replicas/shard1|replica2/digest' in ZooKeeper already exists"
        in node_2.query_and_get_error(
            f"SYSTEM RESTORE DATABASE REPLICA {exclusive_database_name}"
        )
    )

    node_1.query(f"DROP TABLE IF EXISTS {exclusive_database_name}.{test_table} SYNC")

    assert node_1.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}'"
    ) == TSV([0]) 
    assert node_2.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}'"
    ) == TSV([0]) 

    node_1.query(f"DROP DATABASE {exclusive_database_name} SYNC")
    node_2.query(f"DROP DATABASE {exclusive_database_name} SYNC")


def test_restore_db_replica_on_cluster(
    start_cluster,
    exclusive_database_name,
):
    prepare_db(exclusive_database_name)

    test_table_1 = "test_table_1"
    test_table_2 = "test_table_2"

    zk = cluster.get_kazoo_client("zoo1")

    count_test_table = 100

    create_table(node_1, f"{exclusive_database_name}.{test_table_1}")
    fill_table(node_1, f"{exclusive_database_name}.{test_table_1}", count_test_table)

    check_contains_table(
        node_1, f"{exclusive_database_name}.{test_table_1}", count_test_table
    )
    check_contains_table(
        node_2, f"{exclusive_database_name}.{test_table_1}", count_test_table
    )

    zk_rmr_with_retries(zk, f"/clickhouse/{exclusive_database_name}")
    assert zk.exists(f"/clickhouse/{exclusive_database_name}") is None

    restore_database_and_wait(node_1, exclusive_database_name, "test_cluster")

    assert node_1.query(
        f"SELECT count(*) FROM system.databases WHERE name='{exclusive_database_name}'"
    ) == TSV([1]) 

    assert node_1.query(
        f"SELECT count(*) FROM system.tables WHERE database='{exclusive_database_name}'"
    ) == TSV([1]) 

    check_contains_table(
        node_1, f"{exclusive_database_name}.{test_table_1}", count_test_table
    )
    check_contains_table(
        node_2, f"{exclusive_database_name}.{test_table_1}", count_test_table
    )

    create_table(node_1, f"{exclusive_database_name}.{test_table_2}")
    fill_table(node_1, f"{exclusive_database_name}.{test_table_2}", count_test_table)

    check_contains_table(
        node_1, f"{exclusive_database_name}.{test_table_2}", count_test_table
    )
    check_contains_table(
        node_2, f"{exclusive_database_name}.{test_table_2}", count_test_table
    )
