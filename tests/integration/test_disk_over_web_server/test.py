import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node1", main_configs=["configs/storage_conf.xml"], with_nginx=True)
        cluster.add_instance("node2", main_configs=["configs/storage_conf_web.xml"], with_nginx=True)
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_usage(cluster):
    node1 = cluster.instances["node1"]
    node1.query("""
        CREATE TABLE data (id Int32)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = 'def';
    """)
    node1.query("INSERT INTO data SELECT number FROM numbers(100)")
    expected = node1.query("SELECT * FROM data ORDER BY id")

    metadata_path = node1.query("SELECT data_paths FROM system.tables WHERE name='data'")
    metadata_path = metadata_path[metadata_path.find('/'):metadata_path.rfind('/')+1]
    print(f'Metadata: {metadata_path}')

    node1.exec_in_container(['bash', '-c',
                            '/usr/bin/clickhouse web-server-exporter --files-prefix data --url http://nginx:80/test1 --metadata-path {}'.format(metadata_path)], user='root')
    parts = metadata_path.split('/')
    uuid = parts[3]
    print(f'UUID: {uuid}')
    node2 = cluster.instances["node2"]

    node2.query("""
        ATTACH TABLE test1 UUID '{}'
        (id Int32) ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = 'web';
    """.format(uuid))
    result = node2.query("SELECT * FROM test1 ORDER BY id")
    assert(result == expected)
