import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

replica1 = cluster.add_instance(
    "replica1", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)
replica2 = cluster.add_instance(
    "replica2", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def cleanup(nodes):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS source SYNC")
        node.query("DROP TABLE IF EXISTS destination SYNC")


def create_source_table(node, table_name, replicated):
    replica = node.name
    engine = (
        f"ReplicatedMergeTree('/clickhouse/tables/1/{table_name}', '{replica}')"
        if replicated
        else "MergeTree()"
    )
    node.query_with_retry(
        """
        ATTACH TABLE {table_name} UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
        (
        price UInt32,
        date Date,
        postcode1 LowCardinality(String),
        postcode2 LowCardinality(String),
        type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
        is_new UInt8,
        duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
        addr1 String,
        addr2 String,
        street LowCardinality(String),
        locality LowCardinality(String),
        town LowCardinality(String),
        district LowCardinality(String),
        county LowCardinality(String)
        )
        ENGINE = {engine}
        ORDER BY (postcode1, postcode2, addr1, addr2)
        SETTINGS disk = disk(type = web, endpoint = 'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/')
        """.format(
            table_name=table_name, engine=engine
        )
    )


def create_destination_table(node, table_name, replicated):
    replica = node.name
    engine = (
        f"ReplicatedMergeTree('/clickhouse/tables/1/{table_name}', '{replica}')"
        if replicated
        else "MergeTree()"
    )
    node.query_with_retry(
        """
        CREATE TABLE {table_name}
        (
        price UInt32,
        date Date,
        postcode1 LowCardinality(String),
        postcode2 LowCardinality(String),
        type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
        is_new UInt8,
        duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
        addr1 String,
        addr2 String,
        street LowCardinality(String),
        locality LowCardinality(String),
        town LowCardinality(String),
        district LowCardinality(String),
        county LowCardinality(String)
        )
        ENGINE = {engine} 
        ORDER BY (postcode1, postcode2, addr1, addr2)
        """.format(
            table_name=table_name, engine=engine
        )
    )


def test_both_mergetree(start_cluster):
    cleanup([replica1, replica2])
    create_source_table(replica1, "source", False)
    create_destination_table(replica1, "destination", False)

    replica1.query(f"ALTER TABLE destination ATTACH PARTITION tuple() FROM source")

    assert_eq_with_retry(
        replica1,
        f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM destination GROUP BY year ORDER BY year ASC",
        replica1.query(
            f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM source GROUP BY year ORDER BY year ASC"
        ),
    )

    assert_eq_with_retry(
        replica1, f"SELECT town from destination LIMIT 1", "SCARBOROUGH"
    )

    cleanup([replica1])


def test_all_replicated(start_cluster):
    cleanup([replica1, replica2])
    create_source_table(replica1, "source", True)
    create_destination_table(replica1, "destination", True)
    create_destination_table(replica2, "destination", True)

    replica1.query(f"ALTER TABLE destination ATTACH PARTITION tuple() FROM source")
    replica2.query("SYSTEM SYNC REPLICA destination")

    assert_eq_with_retry(
        replica1,
        f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM destination GROUP BY year ORDER BY year ASC",
        replica1.query(
            f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM source GROUP BY year ORDER BY year ASC"
        ),
    )
    assert_eq_with_retry(
        replica1,
        f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM source GROUP BY year ORDER BY year ASC",
        replica2.query(
            f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM destination GROUP BY year ORDER BY year ASC"
        ),
    )

    assert_eq_with_retry(
        replica1, f"SELECT town from destination LIMIT 1", "SCARBOROUGH"
    )

    assert_eq_with_retry(
        replica2, f"SELECT town from destination LIMIT 1", "SCARBOROUGH"
    )

    cleanup([replica1, replica2])


def test_only_destination_replicated(start_cluster):
    cleanup([replica1, replica2])
    create_source_table(replica1, "source", False)
    create_destination_table(replica1, "destination", True)
    create_destination_table(replica2, "destination", True)

    replica1.query(f"ALTER TABLE destination ATTACH PARTITION tuple() FROM source")
    replica2.query("SYSTEM SYNC REPLICA destination")

    assert_eq_with_retry(
        replica1,
        f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM destination GROUP BY year ORDER BY year ASC",
        replica1.query(
            f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM source GROUP BY year ORDER BY year ASC"
        ),
    )
    assert_eq_with_retry(
        replica1,
        f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM source GROUP BY year ORDER BY year ASC",
        replica2.query(
            f"SELECT toYear(date) AS year,round(avg(price)) AS price,bar(price, 0, 1000000, 80) FROM destination GROUP BY year ORDER BY year ASC"
        ),
    )

    assert_eq_with_retry(
        replica1, f"SELECT town from destination LIMIT 1", "SCARBOROUGH"
    )

    assert_eq_with_retry(
        replica2, f"SELECT town from destination LIMIT 1", "SCARBOROUGH"
    )

    cleanup([replica1, replica2])


def test_not_work_on_different_disk(start_cluster):
    cleanup([replica1, replica2])
    # Replace and move should not work on replace
    create_source_table(replica1, "source", False)
    create_destination_table(replica2, "destination", False)

    replica1.query_and_get_error(
        f"ALTER TABLE destination REPLACE PARTITION tuple() FROM source"
    )
    replica1.query_and_get_error(
        f"ALTER TABLE destination MOVE PARTITION tuple() FROM source"
    )
    cleanup([replica1, replica2])
