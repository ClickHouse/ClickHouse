# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/overrides.xml"],
    tmpfs=["/test_dist_conf_disk1:size=100M", "/test_dist_conf_disk2:size=100M"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query("DROP DATABASE IF EXISTS test")
        node.query("CREATE DATABASE test")
        yield cluster
    finally:
        cluster.shutdown()


def _dist_mon_dir(node, root, table):
    """Per-shard queue directory of `table` on `root`, taken from the server's own path.

    An Atomic database keys the data directory by UUID, so it cannot be spelled out here.
    """
    for path in node.query(
        "SELECT arrayJoin(data_paths) FROM system.tables "
        "WHERE database = 'test' AND name = '{table}'".format(table=table)
    ).splitlines():
        if path.startswith("/{root}/".format(root=root)):
            return path.rstrip("/") + "/default@127%2E0%2E0%2E2:9000"
    return None


def _files_in_dist_mon(node, root, table):
    directory = _dist_mon_dir(node, root, table)
    if directory is None:
        return 0
    return int(
        node.exec_in_container(
            [
                "bash",
                "-c",
                # `-maxdepth 1` to avoid /tmp/ subdirectory
                "find '{directory}' -maxdepth 1 -type f 2>/dev/null | wc -l".format(
                    directory=directory
                ),
            ]
        ).split("\n")[0]
    )


def test_insert(start_cluster):
    node.query("DROP TABLE IF EXISTS test.foo")
    node.query("CREATE TABLE test.foo (key Int) Engine=Memory()")
    node.query(
        """
    CREATE TABLE test.dist_foo (key Int)
    Engine=Distributed(
        test_cluster_two_shards,
        test,
        foo,
        key%2,
        'jbod_policy'
    )
    """
    )
    # manual only (but only for remote node)
    node.query("SYSTEM STOP DISTRIBUTED SENDS test.dist_foo")

    node.query(
        "INSERT INTO test.dist_foo SELECT * FROM numbers(100)",
        settings={
            "use_compact_format_in_distributed_parts_names": "0",
        },
    )
    assert _files_in_dist_mon(node, "test_dist_conf_disk1", "dist_foo") == 1
    assert _files_in_dist_mon(node, "test_dist_conf_disk2", "dist_foo") == 0

    assert node.query("SELECT count() FROM test.dist_foo") == "100\n"
    node.query("SYSTEM FLUSH DISTRIBUTED test.dist_foo")
    assert node.query("SELECT count() FROM test.dist_foo") == "200\n"

    #
    # RENAME
    #
    node.query("RENAME TABLE test.dist_foo TO test.dist2_foo")

    node.query(
        "INSERT INTO test.dist2_foo SELECT * FROM numbers(100)",
        settings={
            "use_compact_format_in_distributed_parts_names": "0",
        },
    )
    assert _files_in_dist_mon(node, "test_dist_conf_disk1", "dist2_foo") == 0
    assert _files_in_dist_mon(node, "test_dist_conf_disk2", "dist2_foo") == 1

    assert node.query("SELECT count() FROM test.dist2_foo") == "300\n"
    node.query("SYSTEM FLUSH DISTRIBUTED test.dist2_foo")
    assert node.query("SELECT count() FROM test.dist2_foo") == "400\n"

    #
    # DROP
    #
    # Resolve the directories while the table still exists, otherwise the check below asserts
    # nothing.
    data_paths = node.query(
        "SELECT arrayJoin(data_paths) FROM system.tables "
        "WHERE database = 'test' AND name = 'dist2_foo'"
    ).splitlines()
    assert len(data_paths) == 2, data_paths

    # SYNC: an Atomic database removes the data directory in the background otherwise.
    node.query("DROP TABLE test.dist2_foo SYNC")
    for path in data_paths:
        node.exec_in_container(["bash", "-c", "test ! -e '{}'".format(path)])
