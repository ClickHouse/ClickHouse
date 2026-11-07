import os

import pytest
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
# `user_files_path` of this instance is made a symlink to the directory that holds the files.
node_link = cluster.add_instance("node_link", main_configs=["configs/user_files_link.xml"], stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        for i in range(1, 10):
            node.exec_in_container(
                [
                    "touch",
                    f"/var/lib/clickhouse/user_files/{i}.file",
                ]
            )

        node.exec_in_container(
            [
                "mkdir",
                "/var/lib/clickhouse/user_files/yes",
            ]
        )

        for i in range(20, 30):
            node.exec_in_container(
                [
                    "touch",
                    f"/var/lib/clickhouse/user_files/yes/{i}.file",
                ]
            )

        # Create a controlled directory outside user_files for symlink testing.
        # Using /var/log/clickhouse-server/ would include an unpredictable number of log files.
        # Reset the directory to avoid stale entries from a reused container, which would
        # break the exact row-count assertions below.
        node.exec_in_container(["rm", "-rf", "/tmp/link_target"])
        node.exec_in_container(["mkdir", "-p", "/tmp/link_target"])
        node.exec_in_container(["touch", "/tmp/link_target/test.log"])
        node.exec_in_container(
            [
                "ln",
                "-s",
                "/tmp/link_target/",
                "/var/lib/clickhouse/user_files/link",
            ]
        )

        node_link.stop_clickhouse()
        node_link.exec_in_container(
            [
                "bash",
                "-c",
                "rm -rf /var/lib/clickhouse/user_files_link /var/lib/clickhouse/user_files_real"
                " && mkdir -p /var/lib/clickhouse/user_files_real/sub"
                " && ln -s /var/lib/clickhouse/user_files_real /var/lib/clickhouse/user_files_link"
                " && printf '1,a\\n2,b\\n' > /var/lib/clickhouse/user_files_real/sub/t.csv"
                " && printf '9,secret\\n' > /var/lib/clickhouse/outside.csv",
            ],
            user="root",
        )
        node_link.start_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()


def test_full_path():
    # Expected: root dir (1) + 9 files + yes dir (1) + 10 files in yes + link symlink (1) + test.log in link (1) = 23
    assert (
        node.query("SELECT count() FROM filesystem('/var/lib/clickhouse/user_files/')")
        == "23\n"
    )


def test_file_path():
    assert node.query("SELECT count() FROM filesystem('yes')") == "11\n"


def test_no_path():
    assert node.query("SELECT count() FROM filesystem('')") == "23\n"
    assert (
        node.query(
            "SELECT * FROM filesystem('/var/lib/clickhouse/user_files/') EXCEPT SELECT * FROM filesystem('')"
        )
        == ""
    )


def test_relative_path():
    assert "DATABASE_ACCESS_DENIED" in node.query_and_get_error(
        "SELECT * FROM filesystem('/var/lib/clickhouse/user_files/../')"
    )


def test_escape_path():
    assert (
        node.query(
            "SELECT count() FROM filesystem('/var/lib/clickhouse/user_files/link/test.log')"
        )
        == "1\n"
    )


def test_direct_outside_path():
    """Direct access to a path outside user_files must be denied, even if the same file is reachable through a symlink inside user_files."""
    assert "DATABASE_ACCESS_DENIED" in node.query_and_get_error(
        "SELECT * FROM filesystem('/tmp/link_target/test.log')"
    )


def test_filesystem_database_symlinked_user_files():
    # The `Filesystem` database checked its tables against the unresolved `user_files_path`, while its own path
    # and `file` use the resolved one, so it refused every table when `user_files_path` was a symlink.
    expected = "1\ta\n2\tb\n"
    assert node_link.query("SELECT * FROM file('sub/t.csv') ORDER BY 1") == expected

    node_link.query("CREATE DATABASE fs_relative ENGINE = Filesystem('sub')")
    node_link.query("CREATE DATABASE fs_default ENGINE = Filesystem")
    try:
        assert node_link.query("SELECT * FROM fs_relative.`t.csv` ORDER BY 1") == expected
        assert node_link.query("SELECT * FROM fs_default.`sub/t.csv` ORDER BY 1") == expected
        assert "PATH_ACCESS_DENIED" in node_link.query_and_get_error(
            "SELECT * FROM fs_relative.`../../outside.csv`"
        )
    finally:
        node_link.query("DROP DATABASE IF EXISTS fs_relative")
        node_link.query("DROP DATABASE IF EXISTS fs_default")
