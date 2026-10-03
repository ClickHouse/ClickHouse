import os

import pytest
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
# `user_files_path` of this node is turned into a symlink before the test queries it.
node_symlinked_root = cluster.add_instance(
    "node_symlinked_root",
    main_configs=["configs/symlinked_user_files_path.xml"],
    stay_alive=True,
)


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


def test_symlinked_user_files_path():
    """When `user_files_path` itself is a symlink, an absolute path under the configured
    (symlink) path is inside `user_files`: containment on a plain `user_files_path` is lexical,
    so the prefix must not be canonicalized to the symlink target."""
    node_symlinked_root.stop_clickhouse()
    node_symlinked_root.exec_in_container(
        [
            "bash",
            "-c",
            "rm -rf /var/lib/clickhouse/symlinked_user_files /var/lib/clickhouse/symlinked_user_files_target"
            " && mkdir -p /var/lib/clickhouse/symlinked_user_files_target"
            " && touch /var/lib/clickhouse/symlinked_user_files_target/a.file"
            " && chown -R --reference=/var/lib/clickhouse /var/lib/clickhouse/symlinked_user_files_target"
            " && ln -s /var/lib/clickhouse/symlinked_user_files_target /var/lib/clickhouse/symlinked_user_files",
        ],
        privileged=True,
        user="root",
    )
    node_symlinked_root.start_clickhouse()

    assert (
        node_symlinked_root.query(
            "SELECT name FROM filesystem('/var/lib/clickhouse/symlinked_user_files/') WHERE type = 'regular' ORDER BY name"
        )
        == "a.file\n"
    )
    assert (
        node_symlinked_root.query(
            "SELECT count() FROM filesystem('/var/lib/clickhouse/symlinked_user_files/a.file')"
        )
        == "1\n"
    )
    assert node_symlinked_root.query("SELECT count() FROM filesystem('a.file')") == "1\n"
