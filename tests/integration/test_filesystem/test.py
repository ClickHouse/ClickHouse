import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


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
                f"/var/lib/clickhouse/user_files/yes",
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
