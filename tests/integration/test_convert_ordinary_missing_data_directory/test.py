import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_convert_ordinary_with_missing_data_directory(started_cluster):
    database = "ordinary_missing_data_directory"
    table = "table"

    node.query(
        f"CREATE DATABASE {database} ENGINE=Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(f"CREATE TABLE {database}.{table} (n UInt64) ENGINE=MergeTree ORDER BY n")

    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"rm -rf /var/lib/clickhouse/data/{database}/{table}"],
    )
    node.exec_in_container(
        ["bash", "-c", "touch /var/lib/clickhouse/flags/convert_ordinary_to_atomic"],
    )
    node.start_clickhouse()

    assert "Atomic\n" == node.query(
        f"SELECT engine FROM system.databases WHERE name = '{database}'"
    )
    assert node.contains_in_log(
        f"Creating missing data directory .*{database}/{table}.* before converting database to Atomic"
    )

    node.query(f"DROP DATABASE {database} SYNC")
