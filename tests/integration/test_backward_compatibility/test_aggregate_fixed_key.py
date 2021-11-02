import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="aggregate_fixed_key")
node1 = cluster.add_instance('node1', with_zookeeper=True, image='yandex/clickhouse-server', tag='21.3', with_installed_binary=True)
node2 = cluster.add_instance('node2', with_zookeeper=True, image='yandex/clickhouse-server')
node3 = cluster.add_instance('node3', with_zookeeper=True, image='yandex/clickhouse-server')


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_two_level_merge(start_cluster):
    for node in start_cluster.instances.values():
        node.query(
            """
            CREATE TABLE IF NOT EXISTS test_two_level_merge(date Date, zone UInt32, number UInt32)
            ENGINE = MergeTree() PARTITION BY toUInt64(number / 1000) ORDER BY tuple();

            INSERT INTO
                test_two_level_merge
            SELECT
                toDate('2021-09-28') - number / 1000,
                249081628,
                number
            FROM
                numbers(15000);
            """
        )

    # covers only the keys64 method
    for node in start_cluster.instances.values():
        print(node.query(
            """
            SELECT
                throwIf(uniqExact(date) != count(), 'group by is borked')
            FROM (
                SELECT
                    date
                FROM
                    remote('node{1,2}', default.test_two_level_merge)
                WHERE
                    date BETWEEN toDate('2021-09-20') AND toDate('2021-09-28')
                    AND zone = 249081628
                GROUP by date, zone
            )
            SETTINGS
                group_by_two_level_threshold = 1,
                group_by_two_level_threshold_bytes = 1,
                max_threads = 2,
                prefer_localhost_replica = 0
            """
        ))
