import pytest
import logging

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
            with_zookeeper=True,
            with_remote_database_disk=False,  # The tests work on the local disk and check local files
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


def write(source, disk, path):
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
                    f"{disk}",
                    "--query",
                    f"'write {path}'",
                ]
            ),
            ]
    )


def touch(source, disk, path):
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
                    f"{disk}",
                    "--query",
                    f"'touch {path}'",
                ]
            ),
            ]
    )


def mkdir(source, disk, path):
    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            f"{disk}",
            "--query",
            f"mkdir {path}",
        ]
    )


def ls(source, disk, path):
    return source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            f"{disk}",
            "--query",
            f"list {path}",
        ]
    )

def printPathInfo(source, prefix, disk_path):
    contents = source.exec_in_container(["ls", "-lA --time-style=full-iso", disk_path]).strip()
    logging.info(f"{prefix} Contents of {disk_path}:\n{contents}")

def remove(source, disk, path):
    return source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            f"{disk}",
            "--query",
            f"remove {path}",
        ]
    )


def remove_recurive(source, disk, path):
    return source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--disk",
            f"{disk}",
            "--query",
            f"remove -r {path}",
        ]
    )


def init_data(source):
    source.query("DROP TABLE IF EXISTS test_table")

    source.query(
        "CREATE TABLE test_table(word String, value UInt64) "
        "ENGINE=MergeTree() "
        "ORDER BY word SETTINGS storage_policy = 'test1'"
    )

    source.query("INSERT INTO test_table(*) VALUES ('test1', 2)")


def init_data_s3(source, disk="test3"):
    source.query("DROP TABLE IF EXISTS test_table_s3")

    source.query(
        "CREATE TABLE test_table_s3(word String, value UInt64) "
        "ENGINE=MergeTree() "
        f"ORDER BY word SETTINGS storage_policy = '{disk}'"
    )

    source.query("INSERT INTO test_table_s3(*) VALUES ('test1', 2)")


def init_data_s3_rm_rec(source):
    # a / a / a, b, c
    #   / b / a, b, c, d, e
    #   / c /
    #   / d / a
    mkdir(source, "test3", "a")
    mkdir(source, "test3", "a/a")
    mkdir(source, "test3", "a/b")
    mkdir(source, "test3", "a/c")
    mkdir(source, "test3", "a/d")

    write(source, "test3", "a/a/a")
    write(source, "test3", "a/a/b")
    write(source, "test3", "a/a/c")

    write(source, "test3", "a/b/a")
    write(source, "test3", "a/b/b")
    write(source, "test3", "a/b/c")
    write(source, "test3", "a/b/d")
    write(source, "test3", "a/b/e")

    write(source, "test3", "d/a")


def test_disks_app_func_ld(started_cluster):
    logging.info(f"Start to test test_disks_app_func_ld")
    source = cluster.instances["disks_app_test"]

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--save-logs", "--query", "list-disks"]
    )

    disks = list(
        map(lambda x: x.strip(), filter(lambda x: len(x) > 1, out.split("\n")))
    )

    assert disks == [
        "Initialized disks:",
        "default:/",
        "Uninitialized disks:",
        "local",
        "test1",
        "test2",
        "test3",
        "test4",
    ]

    out = source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--save-logs",
            "--query",
            "switch-disk local; list-disks",
        ]
    )

    disks = list(
        map(lambda x: x.strip(), filter(lambda x: len(x) > 1, out.split("\n")))
    )

    assert disks == [
        "Initialized disks:",
        "default:/",
        "local:/",
        "Uninitialized disks:",
        "test1",
        "test2",
        "test3",
        "test4",
    ]
    logging.info(f"Finish to test test_disks_app_func_ld")


def test_disks_app_func_ls(started_cluster):
    logging.info(f"Start to test test_disks_app_func_ls")
    source = cluster.instances["disks_app_test"]
    printPathInfo(source, "Before remove_recurive in test_disks_app_func_ls", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before remove_recurive in test_disks_app_func_ls", "/var/lib/clickhouse/path2/")

    remove_recurive(source, "test1", ".")
    printPathInfo(source, "After remove_recurive in test_disks_app_func_ls", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After remove_recurive in test_disks_app_func_ls", "/var/lib/clickhouse/path2/")

    init_data(source)

    out = ls(source, "test1", ".")

    files = out.split("\n")

    assert files[0] == "store"

    out = ls(source, "test1", ". --recursive")

    assert ".:\nstore\n" in out
    assert "\n./store:\n" in out
    logging.info(f"Finish to test test_disks_app_func_ls")


def test_disks_app_func_cp(started_cluster):
    logging.info(f"Start to test test_disks_app_func_cp")
    source = cluster.instances["disks_app_test"]
    printPathInfo(source, "Before touch in test_disks_app_func_cp", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before touch in test_disks_app_func_cp", "/var/lib/clickhouse/path2/")

    touch(source, "test1", "path1")
    printPathInfo(source, "After touch in test_disks_app_func_cp", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After touch in test_disks_app_func_cp", "/var/lib/clickhouse/path2/")

    out = ls(source, "test1", ".")

    assert "path1" in out
    printPathInfo(source, "Before clickhosue disks copy in test_disks_app_func_cp", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before clickhosue disks copy in test_disks_app_func_cp", "/var/lib/clickhouse/path2/")

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--query",
            "copy --recursive --disk-from test1 --disk-to test2 . .",
        ]
    )
    printPathInfo(source, "After clickhosue disks copy in test_disks_app_func_cp", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After clickhosue disks copy in test_disks_app_func_cp", "/var/lib/clickhouse/path2/")

    out = ls(source, "test2", ".")

    assert "path1" in out
    printPathInfo(source, "Before remove in test_disks_app_func_cp", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before remove in test_disks_app_func_cp", "/var/lib/clickhouse/path2/")
    remove(source, "test2", "path1")
    remove(source, "test1", "path1")
    printPathInfo(source, "After remove in test_disks_app_func_cp", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After remove in test_disks_app_func_cp", "/var/lib/clickhouse/path2/")

    # alesapin: Why we need list one more time?
    # kssenii: it is an assertion that the file is indeed deleted
    out = ls(source, "test2", ".")

    assert "path1" not in out

    out = ls(source, "test1", ".")

    assert "path1" not in out
    logging.info(f"Finish to test test_disks_app_func_cp")


def test_disks_app_func_ln(started_cluster):
    logging.info(f"Start to test test_disks_app_func_ln")
    source = cluster.instances["disks_app_test"]

    init_data(source)
    printPathInfo(source, "Before clickhosue disks query link in test_disks_app_func_ln", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before clickhosue disks query link in test_disks_app_func_ln", "/var/lib/clickhouse/path2/")

    source.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--query",
            "link data/default/test_table data/default/z_tester",
        ]
    )
    printPathInfo(source, "After clickhosue disks query link in test_disks_app_func_ln", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After clickhosue disks query link in test_disks_app_func_ln", "/var/lib/clickhouse/path2/")

    out = source.exec_in_container(
        ["/usr/bin/clickhouse", "disks", "--save-logs", "--query", "list data/default/"]
    )
    printPathInfo(source, "After clickhosue disks query in test_disks_app_func_ln", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After clickhosue disks query in test_disks_app_func_ln", "/var/lib/clickhouse/path2/")

    files = out.split("\n")

    assert "z_tester" in files
    logging.info(f"Finish to test test_disks_app_func_ln")


def test_disks_app_func_rm(started_cluster):
    logging.info(f"Start to test test_disks_app_func_rm")
    source = cluster.instances["disks_app_test"]
    printPathInfo(source, "After write test 2 in test_disks_app_func_rm", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After write test 2 in test_disks_app_func_rm", "/var/lib/clickhouse/path2/")

    write(source, "test2", "path3")

    printPathInfo(source, "After write test 2 in test_disks_app_func_rm", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After write test 2 in test_disks_app_func_rm", "/var/lib/clickhouse/path2/")

    out = ls(source, "test2", ".")

    assert "path3" in out
    printPathInfo(source, "Before remove test 2 on path3 in test_disks_app_func_rm", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before remove test 2 on path3 in test_disks_app_func_rm", "/var/lib/clickhouse/path2/")

    remove(source, "test2", "path3")

    printPathInfo(source, "After remove test 2 on path3 in test_disks_app_func_rm", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After remove test 2 on path3 in test_disks_app_func_rm", "/var/lib/clickhouse/path2/")

    out = ls(source, "test2", ".")

    assert "path3" not in out
    logging.info(f"Finish to test test_disks_app_func_rm")


def test_disks_app_func_rm_shared_recursive(started_cluster):
    logging.info(f"Start to test test_disks_app_func_rm_shared_recursive")
    source = cluster.instances["disks_app_test"]

    init_data_s3_rm_rec(source)
    out = ls(source, "test3", ". --recursive")
    assert (
            out
            == ".:\na\n\n./a:\na\nb\nc\nd\n\n./a/a:\na\nb\nc\n\n./a/b:\na\nb\nc\nd\ne\n\n./a/c:\n\n./a/d:\n\n"
    )
    printPathInfo(source, "Before rm test 3 recursively in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before rm test 3 recursively in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    remove(source, "test3", "a/a --recursive")
    printPathInfo(source, "After rm test 3 recursively in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After rm test 3 recursively in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    out = ls(source, "test3", ". --recursive")
    assert (
            out == ".:\na\n\n./a:\nb\nc\nd\n\n./a/b:\na\nb\nc\nd\ne\n\n./a/c:\n\n./a/d:\n\n"
    )
    printPathInfo(source, "Before rm test 3 recursively a/b --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before rm test 3 recursively a/b --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    remove(source, "test3", "a/b --recursive")
    printPathInfo(source, "After rm test 3 recursively a/b --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After rm test 3 recursively a/b --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    printPathInfo(source, "Before rm test 3 recursively . --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before rm test 3 recursively . --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    out = ls(source, "test3", ". --recursive")
    assert out == ".:\na\n\n./a:\nc\nd\n\n./a/c:\n\n./a/d:\n\n"
    printPathInfo(source, "After rm test 3 recursively . --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After rm test 3 recursively . --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    printPathInfo(source, "Before rm test 3 recursively a/c --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before rm test 3 recursively a/c --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")


    remove(source, "test3", "a/c --recursive")
    out = ls(source, "test3", ". --recursive")
    assert out == ".:\na\n\n./a:\nd\n\n./a/d:\n\n"
    printPathInfo(source, "After rm test 3 recursively a/c --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After rm test 3 recursively a/c --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    printPathInfo(source, "Before rm test 3 recursively a --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before rm test 3 recursively a --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    remove(source, "test3", "a --recursive")
    out = ls(source, "test3", ". --recursive")
    assert out == ".:\n\n"
    printPathInfo(source, "After rm test 3 recursively a --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After rm test 3 recursively a --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    printPathInfo(source, "Before rm test 3 recursively . --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before rm test 3 recursively . --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

    remove(source, "test3", ". --recursive")
    logging.info(f"Finish to test test_disks_app_func_rm_shared_recursive")

    printPathInfo(source, "After rm test 3 recursively . --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Ater rm test 3 recursively . --recursive in test_disks_app_func_rm_shared_recursive", "/var/lib/clickhouse/path2/")

def test_disks_app_func_mv(started_cluster):
    logging.info(f"Start to test test_disks_app_func_mv")
    source = cluster.instances["disks_app_test"]

    init_data(source)

    out = ls(source, "test1", ".")

    files = out.split("\n")
    assert "old_store" not in files
    assert "store" in files

    printPathInfo(source, "Before run container --query move store old_store test_disks_app_func_mv", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before run container --query move store old_store test_disks_app_func_mv", "/var/lib/clickhouse/path2/")

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
    printPathInfo(source, "After run container --query move store old_store test_disks_app_func_mv", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After run container --query move store old_store test_disks_app_func_mv", "/var/lib/clickhouse/path2/")

    out = ls(source, "test1", ".")

    files = out.split("\n")
    assert "old_store" in files
    assert "store" not in files

    printPathInfo(source, "Before run ./old_store --recursive test_disks_app_func_mv", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before run ./old_store --recursive test_disks_app_func_mv", "/var/lib/clickhouse/path2/")


    remove(source, "test1", "./old_store --recursive")

    printPathInfo(source, "After run ./old_store --recursive test_disks_app_func_mv", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After run ./old_store --recursive test_disks_app_func_mv", "/var/lib/clickhouse/path2/")

    logging.info(f"Finish to test test_disks_app_func_mv")


def test_disks_app_func_read_write(started_cluster):
    logging.info(f"Start to test test_disks_app_func_read_write")
    source = cluster.instances["disks_app_test"]

    printPathInfo(source, "Before write test1 5.txt test_disks_app_func_read_write", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "Before write test1 5.txt  test_disks_app_func_read_write", "/var/lib/clickhouse/path2/")

    write(source, "test1", "5.txt")

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
    printPathInfo(source, "After write test1 5.txt test_disks_app_func_read_write", "/var/lib/clickhouse/path1/")
    printPathInfo(source, "After write test1 5.txt test_disks_app_func_read_write", "/var/lib/clickhouse/path2/")

    files = out.split("\n")

    assert files[0] == "tester"
    logging.info(f"Finish to test test_disks_app_func_read_write")


def test_remote_disk_list(started_cluster):
    logging.info(f"Start to test test_remote_disk_list")
    source = cluster.instances["disks_app_test"]
    init_data_s3(source, "test4")

    out = ls(source, "test4", ".")

    files = out.split("\n")

    assert files[0] == "store"

    out = ls(source, "test4", ". --recursive")

    assert ".:\nstore\n" in out
    assert "\n./store:\n" in out
    logging.info(f"Finish to test test_remote_disk_list")
