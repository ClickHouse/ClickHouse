#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)

STRANDED_PART = "all_0_1_1"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def parts_path(table):
    return f"/clickhouse/tables/{table}/replicas/1/parts"


def remove_part_from_disk(node, table, part_name):
    part_path = node.query(
        f"SELECT path FROM system.parts WHERE table = '{table}' AND name = '{part_name}'"
    ).strip()
    if not part_path:
        raise Exception("Part " + part_name + " doesn't exist")
    node.exec_in_container(["rm", "-r", part_path], privileged=True)


def build_stranded_node(table):
    """Leave a ZooKeeper part node behind with no part in the working set on node1.

    The part directory must be removed only after a covering part is already active: while
    all_0_1_1 is still active, DETACH/ATTACH declares it broken and the replica re-fetches it
    from node2, which dissolves the state under test.
    """
    for node, replica in [(node1, "1"), (node2, "2")]:
        node.query(
            f"CREATE TABLE {table} (n Int32) ENGINE = "
            f"ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
            "ORDER BY n SETTINGS old_parts_lifetime = 100500"
        )

    node1.query(f"INSERT INTO {table} VALUES (1)")
    node1.query(f"INSERT INTO {table} VALUES (2)")
    node1.query(f"OPTIMIZE TABLE {table} FINAL")
    # all_0_1_1 is active now; the second merge makes all_0_2_2 cover it.
    node1.query(f"INSERT INTO {table} VALUES (3)")
    node1.query(f"OPTIMIZE TABLE {table} FINAL")
    node1.query(f"SYSTEM SYNC REPLICA {table}")

    remove_part_from_disk(node1, table, STRANDED_PART)
    node1.query(f"DETACH TABLE {table} SYNC")
    node1.query(f"ATTACH TABLE {table}")
    node1.query(f"SYSTEM WAIT LOADING PARTS {table}")

    # Positive control: without both of these the scenario under test is not reproduced, and any
    # assertion made after the drop would hold for the wrong reason.
    assert (
        node1.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{parts_path(table)}' "
            f"AND name = '{STRANDED_PART}'"
        ).strip()
        == "1"
    )
    assert (
        node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND name = '{STRANDED_PART}'"
        ).strip()
        == "0"
    )


def test_truncate_removes_stranded_zookeeper_part(start_cluster):
    table = "rmt_truncate"
    build_stranded_node(table)

    node1.query(f"TRUNCATE TABLE {table} SETTINGS alter_sync = 2")

    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.zookeeper WHERE path = '{parts_path(table)}'",
        "0\n",
    )

    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")


def test_detach_partition_removes_stranded_zookeeper_part(start_cluster):
    table = "rmt_detach"
    build_stranded_node(table)

    node1.query(f"ALTER TABLE {table} DETACH PARTITION tuple() SETTINGS alter_sync = 2")

    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.zookeeper WHERE path = '{parts_path(table)}' "
        f"AND name = '{STRANDED_PART}'",
        "0\n",
    )

    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")


def test_empty_working_set_removes_stranded_zookeeper_part(start_cluster):
    """A drop range whose working set is already empty must still sweep ZooKeeper.

    The sibling tests keep a covering part, so the removal set is non-empty and the
    empty-set branch of executeDropRange is never taken.
    """
    table = "rmt_empty_set"
    build_stranded_node(table)

    # The first drop empties the working set, but leaves behind an Outdated empty covering part
    # that the removal set still picks up, so wait for the table to hold no part in any state
    # before re-stranding the node. Only the cleanup period matters here: that part is created
    # with remove_time = 0, so old_parts_lifetime never holds it back.
    # system.parts omits Deleting parts unless _state is referenced.
    node1.query(
        f"ALTER TABLE {table} MODIFY SETTING cleanup_delay_period = 1, "
        "max_cleanup_delay_period = 2"
    )
    node1.query(f"TRUNCATE TABLE {table} SETTINGS alter_sync = 2")
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.parts WHERE table = '{table}' AND _state != 'dummy'",
        "0\n",
        retry_count=60,
        sleep_time=1,
    )

    zk = cluster.get_kazoo_client("zoo1")
    zk.create(f"{parts_path(table)}/{STRANDED_PART}", b"")

    # Positive control: the node is back and no part of the range is in the working set, which
    # is what makes the drop below take the empty-set branch rather than the sibling tests' one.
    assert (
        node1.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{parts_path(table)}' "
            f"AND name = '{STRANDED_PART}'"
        ).strip()
        == "1"
    )
    assert (
        node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND _state != 'dummy'"
        ).strip()
        == "0"
    )

    node1.query(f"TRUNCATE TABLE {table} SETTINGS alter_sync = 2")

    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.zookeeper WHERE path = '{parts_path(table)}' "
        f"AND name = '{STRANDED_PART}'",
        "0\n",
    )

    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")
