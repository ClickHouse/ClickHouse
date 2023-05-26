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

        node.exec_in_container(
            [
                "ln",
                "-s",
                f"/var/log/clickhouse-server/",
                f"/var/lib/clickhouse/user_files/link",
            ]
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_full_path():
    assert (
        node.query("SELECT count() FROM filesystem('/var/lib/clickhouse/user_files/')")
        == "22\n"
    )


def test_file_path():
    assert node.query("SELECT count() FROM filesystem('yes')") == "11\n"


def test_no_path():
    assert node.query("SELECT count() FROM filesystem('')") == "22\n"
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
            "SELECT count() FROM filesystem('/var/lib/clickhouse/user_files/link/clickhouse-server.log')"
        )
        == "1\n"
    )
