import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', main_configs=['configs/logs.xml'], stay_alive=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_log_lz4_streaming(started_cluster):

    node.stop_clickhouse()
    node.start_clickhouse()
    node.stop_clickhouse()

    lz4_output = node.exec_in_container(["bash", "-c", "lz4 -t /var/log/clickhouse-server/clickhouse-server.log.lz4 2>&1"])
    assert lz4_output.find('Error') == -1, lz4_output

    compressed_size = int(node.exec_in_container(["bash", "-c", "du -b /var/log/clickhouse-server/clickhouse-server.log.lz4 | awk {' print $1 '}"]))
    uncompressed_size = int(lz4_output.split()[3])
    assert 0 < compressed_size < uncompressed_size, lz4_output
