import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_attach_without_checksums(start_cluster):
    node1.query(
        "CREATE TABLE test (date Date, key Int32, value String) Engine=MergeTree ORDER BY key PARTITION by date"
    )

    node1.query(
        "INSERT INTO test SELECT toDate('2019-10-01'), number, toString(number) FROM numbers(100)"
    )

    assert node1.query("SELECT COUNT() FROM test WHERE key % 10 == 0") == "10\n"

    node1.query("ALTER TABLE test DETACH PARTITION '2019-10-01'")

    assert node1.query("SELECT COUNT() FROM test WHERE key % 10 == 0") == "0\n"
    assert node1.query("SELECT COUNT() FROM test") == "0\n"

    # to be sure output not empty
    node1.exec_in_container(
        [
            "bash",
            "-c",
            'find /var/lib/clickhouse/data/default/test/detached -name "checksums.txt" | grep -e ".*" ',
        ],
        privileged=True,
        user="root",
    )

    node1.exec_in_container(
        [
            "bash",
            "-c",
            'find /var/lib/clickhouse/data/default/test/detached -name "checksums.txt" -delete',
        ],
        privileged=True,
        user="root",
    )

    node1.query("ALTER TABLE test ATTACH PARTITION '2019-10-01'")

    assert node1.query("SELECT COUNT() FROM test WHERE key % 10 == 0") == "10\n"
    assert node1.query("SELECT COUNT() FROM test") == "100\n"
    node1.query("DROP TABLE test")
