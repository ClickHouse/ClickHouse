import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def cleanup():
    node.query("DROP TABLE IF EXISTS source")
    node.query("DROP TABLE IF EXISTS destination")


def create_less_granular_table(name):
    query = f"CREATE TABLE {name} (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(timestamp)"
    node.query(query)


def create_more_granular_table(name):
    query = f"CREATE TABLE {name} (timestamp DateTime) engine=MergeTree ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp)"
    node.query(query)


def test_attach_partition_from_table_with_more_granular_partition_expression_data_not_split(started_cluster):
    cleanup()

    create_more_granular_table("source")
    create_less_granular_table("destination")

    node.query("INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03')")

    node.query("ALTER TABLE destination ATTACH PARTITION '20100302' FROM source")

    source_data = node.query("SELECT * FROM source")
    destination_data = node.query("SELECT * FROM destination")

    assert(source_data == destination_data)


def test_attach_partition_from_table_with_more_granular_partition_expression_data_split(started_cluster):
    cleanup()

    create_less_granular_table("destination")
    create_more_granular_table("source")

    node.query("INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-03 02:01:03')")

    node.query("ALTER TABLE destination ATTACH PARTITION '20100302' FROM source")

    source_data = node.query("SELECT * FROM source")
    destination_data = node.query("SELECT * FROM destination")

    assert(source_data != destination_data)


def test_attach_partition_from_table_with_less_granular_partition_expression_data_not_split(started_cluster):
    cleanup()

    create_more_granular_table("source")
    create_less_granular_table("destination")

    node.query("INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-02 02:01:03')")

    node.query("ALTER TABLE destination ATTACH PARTITION '20100302' FROM source")

    source_data = node.query("SELECT * FROM source")
    destination_data = node.query("SELECT * FROM destination")

    assert(source_data == destination_data)


def test_attach_partition_from_table_with_less_granular_partition_expression_data_split(started_cluster):
    cleanup()

    create_less_granular_table("destination")
    create_more_granular_table("source")

    node.query("INSERT INTO TABLE source VALUES ('2010-03-02 02:01:01'), ('2010-03-03 02:01:03')")

    node.query("ALTER TABLE destination ATTACH PARTITION '20100302' FROM source")

    source_data = node.query("SELECT * FROM source")
    destination_data = node.query("SELECT * FROM destination")

    assert(source_data != destination_data)

