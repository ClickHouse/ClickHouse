

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/zookeeper_config.xml"], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    ('part', 'part_name'),
    [
        ('PARTITION', '2020-08-27'),
        ('PART', '20200827_0_0_0'),
    ]
)
def test_fetch_part_from_allowed_zookeeper(start_cluster, part, part_name):
    node.query(
        "CREATE TABLE simple (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', 'node') ORDER BY tuple() PARTITION BY date;"
    )
    node.query("INSERT INTO simple VALUES ('2020-08-27', 1)")

    node.query(
        "CREATE TABLE simple2 (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/1/simple', 'node') ORDER BY tuple() PARTITION BY date;"
    )

    node.query(
        """ALTER TABLE simple2 FETCH {part} '{part_name}' FROM 'zookeeper2:/clickhouse/tables/0/simple';""".format(
            part=part, part_name=part_name))

    node.query("""ALTER TABLE simple2 ATTACH {part} '{part_name}';""".format(
        part=part, part_name=part_name))

    with pytest.raises(QueryRuntimeException):
        node.query(
            """ALTER TABLE simple2 FETCH {part} '{part_name}' FROM 'zookeeper:/clickhouse/tables/0/simple';""".format(
                part=part, part_name=part_name))

    assert node.query("SELECT id FROM simple2").strip() == "1"
