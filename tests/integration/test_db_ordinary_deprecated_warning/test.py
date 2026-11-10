import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/users.xml",
    ],
    stay_alive = True
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_warning():
    node.query("DROP DATABASE IF EXISTS testdb")
    assert node.query("SELECT name FROM system.databases where engine = 'Ordinary'") == ""
    assert node.query("SELECT count() = 0 FROM system.databases where engine = 'Ordinary'") == "1\n"

    node.query("CREATE DATABASE testdb ENGINE = Ordinary")
    assert node.query("SELECT engine FROM system.databases where name = 'testdb'") == "Ordinary\n"
    assert node.query("SELECT count() = 1 FROM system.warnings where startsWith(message, 'Server has databases (for example `testdb`) with Ordinary engine')") == "1\n"

    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"touch /var/lib/clickhouse/flags/convert_ordinary_to_atomic"]
    )
    node.start_clickhouse()

    assert node.query("SELECT engine FROM system.databases where name = 'testdb'") == "Atomic\n"
    assert node.query("SELECT count() = 0 FROM system.warnings where startsWith(message, 'Server has databases (for example `testdb`) with Ordinary engine')") == "1\n"

    node.query("DROP DATABASE testdb")
    node.stop_clickhouse()
    node.start_clickhouse()
