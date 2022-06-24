from helpers.cluster import ClickHouseCluster

import pytest


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:

        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "disks_app_test",
            main_configs=["config.xml"],
        )

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def init_data(source):
    source.query("DROP TABLE IF EXISTS test_table")

    source.query(
        "CREATE TABLE test_table(word String, value UInt64) "
        "ENGINE=MergeTree() "
        "ORDER BY word SETTINGS storage_policy = 'test1'"
    )

    source.query("INSERT INTO test_table(*) VALUES ('test1', 2)")


def test_disks_app_func_ld(started_cluster):
    source = cluster.instances["disks_app_test"]

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--send-logs", "list-disks"]
    )

    disks = out.split("\n")

    assert disks[0] == "default" and disks[1] == "test1" and disks[2] == "test2"


def test_disks_app_func_ls(started_cluster):
    source = cluster.instances["disks_app_test"]

    init_data(source)

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--send-logs", "--disk", "test1", "list", "."]
    )

    files = out.split("\n")

    assert files[0] == "store"

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--send-logs",
            "--disk",
            "test1",
            "list",
            ".",
            "--recursive",
        ]
    )

    assert ".:\nstore\n" in out
    assert "\n./store:\n" in out


def test_disks_app_func_cp(started_cluster):
    source = cluster.instances["disks_app_test"]

    init_data(source)

    source.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'tester' |"
            + " ".join(
                [
                    "/usr/bin/clickhouse",
                    "disks",
                    "--send-logs",
                    "--disk",
                    "test1",
                    "write",
                    "path1",
                ]
            ),
        ]
    )

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "copy",
            "--diskFrom",
            "test1",
            "--diskTo",
            "test2",
            ".",
            ".",
        ]
    )

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--send-logs", "--disk", "test2", "list", "."]
    )

    assert "path1" in out


def test_disks_app_func_ln(started_cluster):
    source = cluster.instances["disks_app_test"]

    init_data(source)

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "link",
            "data/default/test_table",
            "data/default/z_tester",
        ]
    )

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--send-logs", "list", "data/default/"]
    )

    files = out.split("\n")

    assert "z_tester" in files


def test_disks_app_func_rm(started_cluster):
    source = cluster.instances["disks_app_test"]

    init_data(source)

    source.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'tester' |"
            + " ".join(
                [
                    "/usr/bin/clickhouse",
                    "disks",
                    "--send-logs",
                    "--disk",
                    "test2",
                    "write",
                    "path3",
                ]
            ),
        ]
    )

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--send-logs", "--disk", "test2", "list", "."]
    )

    assert "path3" in out

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--send-logs",
            "--disk",
            "test2",
            "remove",
            "path3",
        ]
    )

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--send-logs", "--disk", "test2", "list", "."]
    )

    assert "path3" not in out


def test_disks_app_func_mv(started_cluster):
    source = cluster.instances["disks_app_test"]

    init_data(source)

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--send-logs", "--disk", "test1", "list", "."]
    )

    files = out.split("\n")
    assert "old_store" not in files
    assert "store" in files

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--disk",
            "test1",
            "move",
            "store",
            "old_store",
        ]
    )

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--send-logs", "--disk", "test1", "list", "."]
    )

    files = out.split("\n")
    assert "old_store" in files
    assert "store" not in files


def test_disks_app_func_read_write(started_cluster):
    source = cluster.instances["disks_app_test"]

    source.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'tester' |"
            + " ".join(
                [
                    "/usr/bin/clickhouse",
                    "disks",
                    "--send-logs",
                    "--disk",
                    "test1",
                    "write",
                    "5.txt",
                ]
            ),
        ]
    )

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--send-logs",
            "--disk",
            "test1",
            "read",
            "5.txt",
        ]
    )

    files = out.split("\n")

    assert files[0] == "tester"
