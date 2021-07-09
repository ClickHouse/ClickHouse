import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('instance')

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_explain_estimates(start_cluster):
    node1.query("CREATE TABLE test (i Int64) ENGINE = MergeTree() ORDER BY i")
    node1.query("INSERT INTO test SELECT number FROM numbers(1000)")
    node1.query("OPTIMIZE TABLE test")
    system_parts_result = node1.query("SELECT any(database), any(table), count() as parts, sum(rows) as rows, sum(marks) as marks, sum(bytes_on_disk) as bytes FROM system.parts WHERE database = 'default' AND table = 'test' and active = 1 GROUP BY (database, table)")
    explain_estimates_result = node1.query("EXPLAIN ESTIMATES SELECT * FROM test")
    assert(system_parts_result == explain_estimates_result)
