import logging
import time
from random import randint

import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    with_minio=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "1"},
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "2"},
)

reading_node = cluster.add_instance(
    "reading_node",
    main_configs=["configs/read_only.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "3"},
)
nodes = [node1, node2]

test_idx = 0


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture
def cleanup():
    yield

    for node in nodes + [reading_node]:
        node.query("drop database if exists re sync;" "drop table if exists system.a;")

    global test_idx
    test_idx += 1


def test_refreshable_mv_in_replicated_db(started_cluster, cleanup):
    for node in nodes:
        # (Use different znode path for each test because even `drop database ... sync` doesn't seem
        # to guarantee that a new database can be immediately created with the same znode path:
        # https://github.com/ClickHouse/ClickHouse/issues/76418 )
        node.query(
            f"create database re engine = Replicated('/test/re_{test_idx}', 'shard1', '{{replica}}');"
        )

    # Table engine check.
    assert "BAD_ARGUMENTS" in node1.query_and_get_error(
        "create materialized view re.a refresh every 1 second (x Int64) engine Memory as select 1 as x"
    )

    # Basic refreshing.
    node1.query(
        "create materialized view re.a refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x as select number*10 as x from numbers(2)"
    )
    node1.query("system sync database replica re")
    for node in nodes:
        node.query("system wait view re.a")
        assert node.query("select * from re.a order by all") == "0\n10\n"
        assert (
            node.query(
                "select database, view, last_success_time != 0, last_refresh_time != 0, last_refresh_replica in ('1','2'), exception from system.view_refreshes"
            )
            == "re\ta\t1\t1\t1\t\n"
        )

    # Append mode, with and without coordination.
    for coordinated in [True, False]:
        name = "append" if coordinated else "append_uncoordinated"
        refresh_settings = "" if coordinated else " settings all_replicas = 1"
        node2.query(
            f"create materialized view re.{name} refresh every 1 year{refresh_settings} append (x Int64) engine ReplicatedMergeTree order by x as select rand() as x"
        )
        # Stop the clocks.
        for node in nodes:
            node.query(
                f"system test view re.{name} set fake time '2040-01-01 00:00:01'"
            )
        # Wait for quiescence.
        for node in nodes:
            # Wait twice to make sure we wait for a refresh that started after we adjusted the clock.
            # Otherwise another refresh may start right after (because clock moved far forward).
            node.query(
                f"system wait view re.{name};\
                system refresh view re.{name};\
                system wait view re.{name};\
                system sync replica re.{name};"
            )
        rows_before = int(nodes[randint(0, 1)].query(f"select count() from re.{name}"))
        # Advance the clocks.
        for node in nodes:
            node.query(
                f"system test view re.{name} set fake time '2041-01-01 00:00:01'"
            )
        # Wait for refresh.
        for node in nodes:
            assert_eq_with_retry(
                node,
                f"select status, last_success_time from system.view_refreshes where view = '{name}'",
                "Scheduled\t2041-01-01 00:00:01",
            )
            node.query(f"system wait view re.{name}")
        # Check results.
        node = nodes[randint(0, 1)]
        node.query(f"system sync replica re.{name}")
        rows_after = int(node.query(f"select count() from re.{name}"))
        expected = 1 if coordinated else 2
        assert rows_after - rows_before == expected

    # Uncoordinated append to unreplicated table.
    node1.query(
        "create materialized view re.unreplicated_uncoordinated refresh every 1 second settings all_replicas = 1 append (x String) engine Memory as select 1 as x"
    )
    node2.query("system sync database replica re")
    for node in nodes:
        node.query("system wait view re.unreplicated_uncoordinated")
        assert (
            node.query("select distinct x from re.unreplicated_uncoordinated") == "1\n"
        )

    # Rename.
    node2.query(
        "create materialized view re.c refresh every 1 year (x Int64) engine ReplicatedMergeTree order by x empty as select rand() as x"
    )
    node1.query("system sync database replica re")
    node1.query("rename table re.c to re.d")
    node1.query(
        "alter table re.d modify query select number + sleepEachRow(1) as x from numbers(5) settings max_block_size = 1"
    )
    # Rename while refreshing.
    node1.query("system refresh view re.d")
    assert_eq_with_retry(
        node2,
        "select status from system.view_refreshes where view = 'd'",
        "RunningOnAnotherReplica",
    )
    node2.query("rename table re.d to re.e")
    node1.query("system wait view re.e")
    assert node1.query("select * from re.e order by x") == "0\n1\n2\n3\n4\n"

    # A view that will be stuck refreshing until dropped.
    node1.query(
        "create materialized view re.f refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x as select sleepEachRow(1) as x from numbers(1000000) settings max_block_size = 1"
    )
    assert_eq_with_retry(
        node2,
        "select status in ('Running', 'RunningOnAnotherReplica') from system.view_refreshes where view = 'f'",
        "1",
    )

    # Locate coordination znodes.
    znode_exists_query = (
        lambda uuid: f"select count() from system.zookeeper where path = '/clickhouse/tables/{uuid}' and name = 'shard1'"
    )
    tables = []
    for row in node1.query(
        "select table, uuid from system.tables where database = 're'"
    ).split("\n")[:-1]:
        name, uuid = row.split("\t")
        print(f"found table {name} {uuid}")
        if name.startswith(".") or name.startswith("_tmp_replace_"):
            continue
        coordinated = not name.endswith("uncoordinated")
        tables.append((name, uuid, coordinated))
        znode_exists = nodes[randint(0, 1)].query(znode_exists_query(uuid)) == "1\n"
        assert coordinated == znode_exists
    assert sorted([name for (name, _, _) in tables]) == [
        "a",
        "append",
        "append_uncoordinated",
        "e",
        "f",
        "unreplicated_uncoordinated",
    ]

    # Drop all tables and check that coordination znodes were deleted.
    for name, uuid, coordinated in tables:
        sync = randint(0, 1) == 0
        nodes[randint(0, 1)].query(f"drop table re.{name}{' sync' if sync else ''}")
        # TODO: After https://github.com/ClickHouse/ClickHouse/issues/61065 is done (for MVs, not ReplicatedMergeTree), check the parent znode instead.
        if sync:
            assert_eq_with_retry(nodes[randint(0, 1)], znode_exists_query(uuid), "0\n")

    # A little stress test dropping MV while it's refreshing, hoping to hit various cases where the
    # drop happens while creating/exchanging/dropping the inner table.
    for i in range(20):
        maybe_empty = " empty" if randint(0, 2) == 0 else ""
        nodes[randint(0, 1)].query(
            f"create materialized view re.g refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x{maybe_empty} as select 1 as x"
        )
        r = randint(0, 5)
        if r == 0:
            pass
        elif r == 1:
            time.sleep(randint(0, 100) / 1000)
        else:
            time.sleep(randint(900, 1100) / 1000)
        nodes[randint(0, 1)].query("drop table re.g")

    # Check that inner and temp tables were dropped.
    for node in nodes:
        assert node.query("show tables from re") == ""


def test_refreshable_mv_in_system_db(started_cluster, cleanup):
    node1.query(
        "create materialized view system.a refresh every 1 second (x Int64) engine Memory as select number+1 as x from numbers(2);"
        "system refresh view system.a;"
    )

    node1.restart_clickhouse()
    node1.query("system refresh view system.a")
    assert node1.query("select count(), sum(x) from system.a") == "2\t3\n"


def test_refreshable_mv_in_read_only_node(started_cluster, cleanup):
    # writable node
    node1.query(
        f"create database re engine = Replicated('/test/re_{test_idx}', 'shard1', '{{replica}}');"
    )

    # read_only node
    reading_node.query(
        f"create database re engine = Replicated('/test/re_{test_idx}', 'shard1', '{{replica}}');"
    )

    # disable view sync on writable node, see if there's RefreshTask on read_only node
    node1.query("system stop view sync")

    # clear text_log ensure all logs are related to this test
    reading_node.query("system flush logs;" "truncate table system.text_log;")

    # this MV will be replicated to read_only node
    node1.query(
        "create materialized view re.a refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x as select number*10 as x from numbers(2)"
    )

    # refresh the view manually
    reading_node.query("system refresh view re.a")

    # slepp 3 seconds to make sure the view is refreshed
    reading_node.query("select sleep(3)")

    # check if there's RefreshTask on read_only node
    reading_node.query("system flush logs")
    assert (
        reading_node.query(
            "select count() from system.text_log where message like '%QUERY_IS_PROHIBITED%'"
        )
        == "0\n"
    )
    assert (
        reading_node.query(
            "select count() from system.view_refreshes where exception != ''"
        )
        == "0\n"
    )

    # start sync and chek refresh task works well on node1
    node1.query("system start view sync")
    node1.query("system refresh view re.a")
    assert_eq_with_retry(
        node1,
        "select * from re.a order by x",
        "0\n10\n",
    )
    assert_eq_with_retry(
        node1,
        "select count() from system.view_refreshes where exception = '' and last_refresh_replica = '1'",
        "1\n",
    )


def test_refreshable_mv_in_read_only_node_no_ddl(started_cluster, cleanup):
    node1.query(
        f"create database re engine = Replicated('/test/re_{test_idx}', 'shard1', '{{replica}}');"
    )

    reading_node.query(
        f"create database re engine = Replicated('/test/re_{test_idx}', 'shard1', '{{replica}}');"
    )

    reading_node.replace_in_config(
        "/etc/clickhouse-server/users.d/users.xml", "<allow_ddl>1", "<allow_ddl>0"
    )
    reading_node.query("SYSTEM RELOAD CONFIG")

    assert "DDL queries are prohibited" in reading_node.query_and_get_error(
        "create table foo (x Int64) engine = Null;"
    )

    node1.query(
        "create table re.foo (x Int64) engine = ReplicatedMergeTree order by x;"
    )

    node1.query("system stop view sync")

    # this MV will be replicated to read_only node
    node1.query(
        "create materialized view re.a refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x as select number*10 as x from numbers(2)",
    )

    reading_node.query("system refresh view re.a")

    # slepp 3 seconds to make sure the view is refreshed
    reading_node.query("select sleep(3)")

    # check if there's RefreshTask on read_only node
    assert (
        reading_node.query(
            "select count() from system.view_refreshes where exception != ''"
        )
        == "0\n"
    )

    assert_eq_with_retry(
        reading_node,
        "select * from re.a order by x",
        "0\n10\n",
    )

    # for cleanup
    reading_node.replace_in_config(
        "/etc/clickhouse-server/users.d/users.xml", "<allow_ddl>0", "<allow_ddl>1"
    )
    reading_node.query("SYSTEM RELOAD CONFIG")


def test_refresh_vs_shutdown_smoke(started_cluster, cleanup):
    for node in nodes:
        node.query(
            f"create database re engine = Replicated('/test/re_{test_idx}', 'shard1', '{{replica}}');"
        )

    node1.stop_clickhouse()

    num_tables = 2

    for i in range(10):
        exec_id = node1.start_clickhouse()
        assert exec_id is not None

        if i == 0:
            node1.query("select '===test_refresh_vs_shutdown_smoke start==='")
            for j in range(num_tables):
                node1.query(
                    f"create materialized view re.a{j} refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x as select number*10 as x from numbers(2)"
                )

        if randint(0, 1):
            for j in range(num_tables):
                if randint(0, 1):
                    node1.query(f"system refresh view re.a{j}")
        r = randint(0, 2)
        if r == 1:
            time.sleep(randint(0, 10) / 1000)
        elif r == 2:
            time.sleep(randint(0, 100) / 1000)

        node1.stop_clickhouse(stop_wait_sec=300)
        while True:
            exit_code = cluster.docker_client.api.exec_inspect(exec_id)["ExitCode"]
            if exit_code is not None:
                assert exit_code == 0
                break
            time.sleep(1)

    assert not node1.contains_in_log("view refreshes failed to stop", from_host=True)
    assert not node1.contains_in_log("Closed connections. But", from_host=True)
    assert not node1.contains_in_log("Will shutdown forcefully.", from_host=True)
    assert not node1.contains_in_log("##########", from_host=True)
    assert node1.contains_in_log(
        "===test_refresh_vs_shutdown_smoke start===", from_host=True
    )

    node1.start_clickhouse()


def test_pause(started_cluster, cleanup):
    for node in nodes:
        node.query(
            "create database re engine = Replicated('/test/re', 'shard1', '{replica}');"
        )
    node1.query(
        "create table re.src (x Int64) engine ReplicatedMergeTree order by x;"
        "insert into re.src values (1);"
    )
    node2.query(
        "system sync database replica re;"
        "system sync replica re.src;"
        "create materialized view re.a refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x as select x from re.src;"
        "system wait view re.a"
    )
    assert node2.query("select * from re.a") == "1\n"
    node2.query("system stop replicated view re.a")
    node1.restart_clickhouse()  # just to guarantee that it notices the new znode
    try:
        node2.query("system wait view re.a")
    except QueryRuntimeException as ex:
        # If the node1.restart_clickhouse() interrupted a refresh, the error message (with substring
        # "cancelled") is written to keeper, then thrown by "system wait view". That's normal.
        assert "cancelled" in str(ex)
    node2.query("truncate table re.src;" "insert into re.src values (2);")
    time.sleep(3)
    assert node1.query("select * from re.a") == "1\n"
    node1.query("system start replicated view re.a")
    assert_eq_with_retry(
        node1,
        "select * from re.a",
        "2\n",
    )
    # Drop while paused.
    node1.query("system stop replicated view re.a")
    for node in nodes:
        node.query("drop database re sync")


backup_id_counter = 0


def new_backup_destination():
    global backup_id_counter
    backup_id_counter += 1
    backup_name = f"backup{backup_id_counter}"

    return f"S3('http://minio1:9001/root/data/backups/{backup_name}', 'minio', 'ClickHouse_Minio_P@ssw0rd')"


def do_test_backup(to_table):
    for node in nodes:
        node.query(
            "create database re engine = Replicated('/test/re', 'shard1', '{replica}');"
        )

    target = "rmv"
    if to_table:
        node1.query(
            "create table re.tgt (x Int64) engine ReplicatedMergeTree order by x"
        )
        target = "tgt"

    node1.query(
        "create table re.src (x Int64) engine ReplicatedMergeTree order by x;"
        "insert into re.src values (1);"
    )
    node2.query(
        "system sync database replica re;"
        "system sync replica re.src;"
        f"create materialized view re.rmv refresh every 1 second {'TO re.tgt' if to_table else '(x Int64) engine ReplicatedMergeTree order by x'} as select x from re.src;"
        "system wait view re.rmv"
    )
    assert node2.query(f"select * from re.{target}") == "1\n"

    backup_destination = new_backup_destination()
    tables_exist_query = "SELECT count() FROM system.tables where database='re' AND name in ('src', 'rmv')"

    node1.query(
        f"BACKUP ALL EXCEPT DATABASE system ON CLUSTER 'default' TO {backup_destination};"
    )
    for node in nodes:
        node.query(
            "drop database re sync;"
            "drop database if exists re_broken_replicated_tables sync;"
        )
    assert node1.query(tables_exist_query) == "0\n"

    node1.query(f"RESTORE ALL ON CLUSTER 'default' FROM {backup_destination};")

    assert node1.query(tables_exist_query) == "2\n"
    assert node2.query(tables_exist_query) == "2\n"
    if not to_table:
        # Inner tables are not backed up, wait for first refresh.
        node1.query(f"SYSTEM WAIT VIEW re.{target}")
        node2.query(f"SYSTEM WAIT VIEW re.{target}")
    else:
        node1.query(f"SYSTEM SYNC REPLICA re.{target}")
        node2.query(f"SYSTEM SYNC REPLICA re.{target}")
    assert node1.query(f"SELECT * FROM re.{target}") == "1\n"
    assert node2.query(f"SELECT * FROM re.{target}") == "1\n"

    node1.query("insert into re.src values (2)")
    assert_eq_with_retry(
        node2,
        f"select * from re.{target} order by x",
        "1\n2\n",
    )


def test_backup_outer_table(started_cluster, cleanup):
    do_test_backup(True)


def test_backup_inner_table(started_cluster, cleanup):
    do_test_backup(False)


def test_adding_replica(started_cluster, cleanup):
    node1.query(
        "create database re engine = Replicated('/test/re', 'shard1', 'r1');"
        "create materialized view re.a refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x as select number*10 as x from numbers(2);"
        "system wait view re.a"
    )
    assert node1.query("select * from re.a order by all") == "0\n10\n"
    assert (
        node1.query("select last_refresh_replica from system.view_refreshes") == "1\n"
    )

    r = node2.query(
        "create database re engine = Replicated('/test/re', 'shard1', 'r2');"
        "system sync database replica re"
    )
    assert node2.query("select * from re.a order by all") == "0\n10\n"

    node1.query("system stop view re.a")
    node2.query_with_retry(
        "select last_refresh_replica from system.view_refreshes",
        check_callback=lambda x: x == "2\n",
        sleep_time=1,
        retry_count=20,
    )


def test_replicated_db_startup_race(started_cluster, cleanup):
    for node in nodes:
        node.query(
            f"create database re engine = Replicated('/test/re_{test_idx}', 'shard1', '{{replica}}');"
        )
    node1.query(
        "create materialized view re.a refresh every 1 second (x Int64) engine ReplicatedMergeTree order by x as select number*10 as x from numbers(2);\
            system wait view re.a"
    )

    # Drop a database before it's loaded.
    # We stall DatabaseReplicated::startupDatabaseAsync task and expect the server to become responsive without waiting for it.
    node1.replace_in_config(
        "/etc/clickhouse-server/config.d/config.xml",
        "<database_replicated_startup_pause>false</database_replicated_startup_pause>",
        "<database_replicated_startup_pause>true</database_replicated_startup_pause>",
    )
    node1.restart_clickhouse()
    node1.replace_in_config(
        "/etc/clickhouse-server/config.d/config.xml",
        "<database_replicated_startup_pause>true</database_replicated_startup_pause>",
        "<database_replicated_startup_pause>false</database_replicated_startup_pause>",
    )
    drop_query_handle = node1.get_query_request(
        "drop database re sync"
    )  # this will get stuck until we unpause loading
    time.sleep(2)
    node1.query("system disable failpoint database_replicated_startup_pause")
    _, err = drop_query_handle.get_answer_and_error()
    assert err == ""
