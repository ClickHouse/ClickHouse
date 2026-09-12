import random
import string
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
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

    # SYSTEM SYNC REPLICA waits for the background fetch of all parts after the
    # restore, instead of a fixed-budget count poll that can expire on slow builds.
    check_contains_table(
        node_1, f"{exclusive_database_name}.{test_table_1}", count_test_table_1
    )
    check_contains_table(
        node_2, f"{exclusive_database_name}.{test_table_1}", count_test_table_1
    )

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


def test_restore_db_replica_waits_for_in_flight_ddl(
    start_cluster,
    exclusive_database_name,
):
    fail_point = "database_replicated_pause_before_initial_entry_execution"
    seed_table = "seed_table"
    parked_table = "parked_table"

    # One replica only: the test deletes this replica's Keeper state to make the restore legal, and a
    # second replica would change what the restore recovers from.
    node_1.query(
        f"""
            CREATE DATABASE {exclusive_database_name}
            ENGINE=Replicated("/clickhouse/{exclusive_database_name}", \'{{shard}}\', \'{{replica}}\')
        """
    )
    create_table(node_1, f"{exclusive_database_name}.{seed_table}")

    zk = cluster.get_kazoo_client("zoo1")
    replica_path = f"/clickhouse/{exclusive_database_name}/replicas/shard1|replica1"

    restore_query_id = f"restore_{generate_random_string(10)}"

    finished = []
    finished_lock = threading.Lock()
    create_result = {}
    restore_result = {}

    def run_query(query, sink, label, query_id=None):
        try:
            node_1.query(query, query_id=query_id)
            sink["ok"] = True
        except Exception as ex:
            sink["error"] = str(ex)
        with finished_lock:
            finished.append(label)

    create_thread = threading.Thread(
        target=run_query,
        args=(
            f"CREATE TABLE {exclusive_database_name}.{parked_table} (n UInt64) ENGINE = MergeTree ORDER BY n",
            create_result,
            "create",
        ),
    )
    restore_thread = threading.Thread(
        target=run_query,
        args=(
            f"SYSTEM RESTORE DATABASE REPLICA {exclusive_database_name}",
            restore_result,
            "restore",
            restore_query_id,
        ),
    )

    try:
        node_1.query(f"SYSTEM ENABLE FAILPOINT {fail_point}")
        create_thread.start()

        # Blocks until the CREATE is parked inside tryEnqueueAndExecuteEntry, still holding
        # the table-level DDLGuard that InterpreterCreateQuery took for it.
        node_1.query(f"SYSTEM WAIT FAILPOINT {fail_point} PAUSE", timeout=60)

        # A healthy database refuses the restore ("digest ... already exists"), so drop this
        # replica's Keeper state first. /log survives, so the parked entry can still commit.
        zk_rmr_with_retries(zk, replica_path)
        assert zk.exists(replica_path) is None

        restore_thread.start()

        # The restore must start and then stay unfinished for as long as the entry is parked.
        # Both halves are latched, so neither can be missed by a slow poll: once the restore
        # finishes it stays finished, and once it recreates its Keeper node that node stays.
        # Blocked on the guard it never finishes at all, so the window cannot be too short.
        observed_running = False
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            with finished_lock:
                assert "restore" not in finished, (
                    "SYSTEM RESTORE DATABASE REPLICA finished while a replicated DDL entry"
                    f" was still executing through the DDL worker it replaces: {restore_result}"
                )
            assert zk.exists(replica_path) is None, (
                "SYSTEM RESTORE DATABASE REPLICA recreated the replica in Keeper while a"
                " replicated DDL entry was still executing through the DDL worker it replaces"
            )
            if not observed_running:
                observed_running = (
                    node_1.query(
                        "SELECT count() FROM system.processes"
                        f" WHERE query_id = '{restore_query_id}'"
                    ).strip()
                    == "1"
                )
            time.sleep(0.2)

        assert observed_running, (
            "SYSTEM RESTORE DATABASE REPLICA was never observed running, so the window it"
            f" is supposed to block in was never entered. finished={finished}"
        )

        node_1.query(f"SYSTEM DISABLE FAILPOINT {fail_point}")

        create_thread.join(timeout=120)
        restore_thread.join(timeout=120)
        assert not create_thread.is_alive(), "the parked CREATE TABLE never returned"
        assert not restore_thread.is_alive(), (
            "SYSTEM RESTORE DATABASE REPLICA never returned after the entry was released;"
            f" finished={finished}"
        )

        assert restore_result == {"ok": True}, f"restore failed: {restore_result}"
        # The entry loses the race for the database lock the restore now holds exclusively,
        # exactly as it does against the pre-existing guard in DROP DATABASE.
        create_error = create_result.get("error", "")
        assert (
            "Code: 159" in create_error
            and "Unable to acquire the database lock" in create_error
        ), f"expected the parked entry to fail with TIMEOUT_EXCEEDED on the database lock, got: {create_result}"
        assert node_1.query("SELECT 1").strip() == "1"
        assert zk.exists(replica_path)
        assert seed_table in get_tables_from_replicated(node_1, exclusive_database_name)
    finally:
        # Disable first, so a failed assertion above cannot leave a thread parked forever.
        try:
            node_1.query(f"SYSTEM DISABLE FAILPOINT {fail_point}")
        except Exception as ex:
            print(ex)
        for thread in (create_thread, restore_thread):
            if thread.ident is not None:
                thread.join(timeout=120)
        try:
            node_1.query(f"DROP DATABASE IF EXISTS {exclusive_database_name} SYNC")
        except Exception as ex:
            print(ex)


def test_restore_db_replica_waits_for_database_sync(
    start_cluster,
    exclusive_database_name,
):
    # SYSTEM SYNC DATABASE REPLICA holds the database-wide DDLGuard while it waits, and that guard
    # does not imply the shared database lock, so a waiting sync isolates it from the exclusive
    # lock the restore takes next.
    fail_point = "database_replicated_stop_entry_execution"
    remote_table = "remote_table"

    prepare_db(exclusive_database_name)
    for node in cluster_nodes:
        node.query(f"SYSTEM SYNC DATABASE REPLICA {exclusive_database_name}")

    zk = cluster.get_kazoo_client("zoo1")
    digest_path = (
        f"/clickhouse/{exclusive_database_name}/replicas/shard1|replica1/digest"
    )

    sync_query_id = f"sync_{generate_random_string(10)}"
    restore_query_id = f"restore_{generate_random_string(10)}"

    finished = []
    finished_lock = threading.Lock()
    sync_result = {}
    restore_result = {}

    def run_query(query, sink, label, query_id, settings=None):
        try:
            node_1.query(query, query_id=query_id, settings=settings)
            sink["ok"] = True
        except Exception as ex:
            sink["error"] = str(ex)
        with finished_lock:
            finished.append(label)

    sync_thread = threading.Thread(
        target=run_query,
        args=(
            f"SYSTEM SYNC DATABASE REPLICA {exclusive_database_name}",
            sync_result,
            "sync",
            sync_query_id,
            {"receive_timeout": 120},
        ),
    )
    restore_thread = threading.Thread(
        target=run_query,
        args=(
            f"SYSTEM RESTORE DATABASE REPLICA {exclusive_database_name}",
            restore_result,
            "restore",
            restore_query_id,
        ),
    )

    try:
        node_1.query(f"SYSTEM ENABLE FAILPOINT {fail_point}")

        # The entry is enqueued from the other replica, so no query thread on node_1 holds a
        # table-level DDLGuard: this failpoint parks node_1's own worker thread before it takes
        # one. That is what makes the arm measure the database-wide guard and nothing else.
        node_2.query(
            f"CREATE TABLE {exclusive_database_name}.{remote_table} (n UInt32)"
            " ENGINE = ReplicatedMergeTree ORDER BY n",
            settings={
                "distributed_ddl_task_timeout": 5,
                "distributed_ddl_output_mode": "never_throw",
            },
        )
        node_1.query(f"SYSTEM WAIT FAILPOINT {fail_point} PAUSE", timeout=60)

        # node_1 is behind the log now, so the sync waits instead of returning at once.
        sync_thread.start()
        # `getDDLGuard(db, "")` is the first statement of `syncReplicatedDatabase`, and this
        # line is logged after it and immediately before the wait, so seeing it for this query
        # id proves the sync holds the guard.
        sync_latch = (
            f"{{{sync_query_id}}} <Trace> InterpreterSystemQuery: Synchronizing entries"
        )
        deadline = time.monotonic() + 60
        while not node_1.contains_in_log(sync_latch):
            assert time.monotonic() < deadline, (
                f"SYSTEM SYNC DATABASE REPLICA never reached its wait: {sync_result}"
            )
            time.sleep(0.2)

        digest_before_restore = zk.get(digest_path)[0]
        restore_thread.start()

        # Latched, as in the test above. The digest is the condition that fires: an unguarded
        # restore rewrites it to force recovery, and only then reaches the worker reset, where it
        # blocks on the parked thread - so "did not finish" alone would pass unguarded too. The
        # sync half keeps the arm honest, because a sync that stopped waiting holds no guard.
        observed_running = False
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            with finished_lock:
                assert "sync" not in finished, (
                    "SYSTEM SYNC DATABASE REPLICA stopped waiting before the window ended, so"
                    f" this arm measured nothing: {sync_result}"
                )
                assert "restore" not in finished, (
                    "SYSTEM RESTORE DATABASE REPLICA finished while SYSTEM SYNC DATABASE REPLICA"
                    f" held the database guard: {restore_result}"
                )
            assert zk.get(digest_path)[0] == digest_before_restore, (
                "SYSTEM RESTORE DATABASE REPLICA rewrote this replica's digest in Keeper during"
                " the window in which SYSTEM SYNC DATABASE REPLICA was waiting"
            )
            if not observed_running:
                observed_running = (
                    node_1.query(
                        "SELECT count() FROM system.processes"
                        f" WHERE query_id = '{restore_query_id}'"
                    ).strip()
                    == "1"
                )
            time.sleep(0.2)

        assert observed_running, (
            "SYSTEM RESTORE DATABASE REPLICA was never observed running, so the window it"
            f" is supposed to block in was never entered. finished={finished}"
        )

        node_1.query(f"SYSTEM DISABLE FAILPOINT {fail_point}")

        sync_thread.join(timeout=180)
        restore_thread.join(timeout=180)
        assert not sync_thread.is_alive(), "SYSTEM SYNC DATABASE REPLICA never returned"
        assert not restore_thread.is_alive(), (
            "SYSTEM RESTORE DATABASE REPLICA never returned after the sync released the"
            f" database guard; finished={finished}"
        )

        assert sync_result == {"ok": True}, f"sync failed: {sync_result}"
        # Once released the restore reaches the same refusal it gives on any healthy replica, so
        # it was delayed and not broken.
        assert (
            f"Replica node '/clickhouse/{exclusive_database_name}/replicas/shard1|replica1/digest'"
            " in ZooKeeper already exists" in restore_result.get("error", "")
        ), f"expected the restore to refuse a healthy replica, got: {restore_result}"
        assert remote_table in get_tables_from_replicated(node_1, exclusive_database_name)
    finally:
        # Disable first, so a failed assertion above cannot leave a thread parked forever.
        try:
            node_1.query(f"SYSTEM DISABLE FAILPOINT {fail_point}")
        except Exception as ex:
            print(ex)
        for thread in (sync_thread, restore_thread):
            if thread.ident is not None:
                thread.join(timeout=180)
        for node in cluster_nodes:
            try:
                node.query(f"DROP DATABASE IF EXISTS {exclusive_database_name} SYNC")
            except Exception as ex:
                print(ex)

    assert node_1.query(
        f"SELECT count() FROM system.fail_points WHERE name = '{fail_point}' AND enabled"
    ) == TSV([0])


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
