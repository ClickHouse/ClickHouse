import os
import re
import shutil
import threading
import time
from random import randint

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain

test_recover_staled_replica_run = 1

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    macros={"shard": "shard1", "replica": "1"},
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    macros={"shard": "shard1", "replica": "2"},
)
nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_refreshable_mv_in_replicated_db(started_cluster):
    for node in nodes:
        node.query(
            "create database re engine = Replicated('/test/re', 'shard1', '{replica}');"
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
                f"system wait view re.{name}; system refresh view re.{name}; system wait view re.{name};"
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
    znode_exists = (
        lambda uuid: nodes[randint(0, 1)].query(
            f"select count() from system.zookeeper where path = '/clickhouse/tables/{uuid}' and name = 'shard1'"
        )
        == "1\n"
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
        assert coordinated == znode_exists(uuid)
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
        maybe_sync = " sync" if randint(0, 1) == 0 else ""
        nodes[randint(0, 1)].query(f"drop table re.{name}{maybe_sync}")
        # TODO: After https://github.com/ClickHouse/ClickHouse/issues/61065 is done (for MVs, not ReplicatedMergeTree), check the parent znode instead.
        assert not znode_exists(uuid)

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

    node1.query("drop database re sync")
    node2.query("drop database re sync")


def test_refreshable_mv_in_system_db(started_cluster):
    node1.query(
        "create materialized view system.a refresh every 1 second (x Int64) engine Memory as select number+1 as x from numbers(2);"
        "system refresh view system.a;"
    )

    node1.restart_clickhouse()
    node1.query("system refresh view system.a")
    assert node1.query("select count(), sum(x) from system.a") == "2\t3\n"

    node1.query("drop table system.a")
