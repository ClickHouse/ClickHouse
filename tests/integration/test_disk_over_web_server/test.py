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
    expected = ""
    uuids = []
    for i in range(3):
        node1.query(""" CREATE TABLE data{} (id Int32) ENGINE = MergeTree() ORDER BY id SETTINGS storage_policy = 'def';""".format(i))
        node1.query("INSERT INTO data{} SELECT number FROM numbers(500000 * {})".format(i, i + 1))
        expected = node1.query("SELECT * FROM data{} ORDER BY id".format(i))

        metadata_path = node1.query("SELECT data_paths FROM system.tables WHERE name='data{}'".format(i))
        metadata_path = metadata_path[metadata_path.find('/'):metadata_path.rfind('/')+1]
        print(f'Metadata: {metadata_path}')

        node1.exec_in_container(['bash', '-c',
                                '/usr/bin/clickhouse web-server-exporter --files-prefix data --url http://nginx:80/test1 --metadata-path {}'.format(metadata_path)], user='root')
        parts = metadata_path.split('/')
        uuids.append(parts[3])
        print(f'UUID: {parts[3]}')

    node2 = cluster.instances["node2"]
    for i in range(3):
        node2.query("""
            ATTACH TABLE test{} UUID '{}'
            (id Int32) ENGINE = MergeTree() ORDER BY id
            SETTINGS storage_policy = 'web';
        """.format(i, uuids[i], i, i))

        result = node2.query("SELECT count() FROM test{}".format(i))
        assert(int(result) == 500000 * (i+1))

        result = node2.query("SELECT id FROM test{} WHERE id % 56 = 3 ORDER BY id".format(i))
        assert(result == node1.query("SELECT id FROM data{} WHERE id % 56 = 3 ORDER BY id".format(i)))

        result = node2.query("SELECT id FROM test{} WHERE id > 789999 AND id < 999999 ORDER BY id".format(i))
        assert(result == node1.query("SELECT id FROM data{} WHERE id > 789999 AND id < 999999 ORDER BY id".format(i)))

        print(f"Ok {i}")
