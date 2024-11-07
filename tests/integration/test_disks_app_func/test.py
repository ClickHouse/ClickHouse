import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "disks_app_test",
            main_configs=["config.xml"],
            with_minio=True,
        )
        cluster.start()

        # local disk requires its `path` directory to exist.
        # the two paths below belong to `test1` and `test2` disks
        node = cluster.instances["disks_app_test"]
        for path in ["path1", "path2"]:
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"mkdir -p /var/lib/clickhouse/{path}",
                ]
            )

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


def init_data_s3(source):
    source.query("DROP TABLE IF EXISTS test_table_s3")

    source.query(
        "CREATE TABLE test_table_s3(word String, value UInt64) "
        "ENGINE=MergeTree() "
        "ORDER BY word SETTINGS storage_policy = 'test3'"
    )

    source.query("INSERT INTO test_table_s3(*) VALUES ('test1', 2)")


def test_disks_app_func_ld(started_cluster):
    source = cluster.instances["disks_app_test"]

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--save-logs", "--query", "list-disks"]
    )

    disks = list(
        sorted(
            map(
                lambda x: x.split(":")[0], filter(lambda x: len(x) > 1, out.split("\n"))
            )
        )
    )

    assert disks[:4] == ["default", "local", "test1", "test2"]


def test_disks_app_func_ls(started_cluster):
    source = cluster.instances["disks_app_test"]

    init_data(source)

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test1",
            "--query",
            "list .",
        ]
    )

    files = out.split("\n")

    assert files[0] == "store"

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test1",
            "--query",
            "list . --recursive",
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
                    "--save-logs",
                    "--disk",
                    "test1",
                    "--query",
                    "'write path1'",
                ]
            ),
        ]
    )

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--query",
            "copy --recursive --disk-from test1 --disk-to test2 . .",
        ]
    )

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test2",
            "--query",
            "list .",
        ]
    )

    assert "path1" in out

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test2",
            "--query",
            "remove path1",
        ]
    )

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test1",
            "--query",
            "remove path1",
        ]
    )

    # alesapin: Why we need list one more time?
    # kssenii: it is an assertion that the file is indeed deleted
    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test2",
            "--query",
            "list .",
        ]
    )

    assert "path1" not in out

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test1",
            "--query",
            "list .",
        ]
    )

    assert "path1" not in out


def test_disks_app_func_ln(started_cluster):
    source = cluster.instances["disks_app_test"]

    init_data(source)

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--query",
            "link data/default/test_table data/default/z_tester",
        ]
    )

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--save-logs", "--query", "list data/default/"]
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
                    "--save-logs",
                    "--disk",
                    "test2",
                    "--query",
                    "'write path3'",
                ]
            ),
        ]
    )

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test2",
            "--query",
            "list .",
        ]
    )

    assert "path3" in out

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test2",
            "--query",
            "remove path3",
        ]
    )

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test2",
            "--query",
            "list .",
        ]
    )

    assert "path3" not in out


def test_disks_app_func_mv(started_cluster):
    source = cluster.instances["disks_app_test"]

    init_data(source)

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test1",
            "--query",
            "list .",
        ]
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
            "--query",
            "move store old_store",
        ]
    )

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test1",
            "--query",
            "list .",
        ]
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
                    "--save-logs",
                    "--disk",
                    "test1",
                    "--query",
                    "'write 5.txt'",
                ]
            ),
        ]
    )

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test1",
            "--query",
            "read 5.txt",
        ]
    )

    files = out.split("\n")

    assert files[0] == "tester"


def test_remote_disk_list(started_cluster):
    source = cluster.instances["disks_app_test"]
    init_data_s3(source)

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test3",
            "--query",
            "list .",
        ]
    )

    files = out.split("\n")

    assert files[0] == "store"

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            "test3",
            "--query",
            "list . --recursive",
        ]
    )

    assert ".:\nstore\n" in out
    assert "\n./store:\n" in out
