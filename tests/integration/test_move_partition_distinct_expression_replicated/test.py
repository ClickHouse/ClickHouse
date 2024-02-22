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


def create_table(node, table_name, replicated):
    replica = node.name
    engine = (
        f"ReplicatedMergeTree('/clickhouse/tables/1/{table_name}', '{replica}')"
        if replicated
        else "MergeTree()"
    )
    partition_expression = (
        "toYYYYMMDD(timestamp)" if table_name == "source" else "toYYYYMM(timestamp)"
    )
    node.query_with_retry(
        """
        CREATE TABLE {table_name}(timestamp DateTime)
        ENGINE = {engine}
        ORDER BY tuple() PARTITION BY {partition_expression}
        SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, allow_experimental_alter_partition_with_different_key=1;
        """.format(
            table_name=table_name,
            engine=engine,
            partition_expression=partition_expression,
        )
    )


def test_both_replicated(start_cluster):
    cleanup([replica1, replica2])

    for node in [replica1, replica2]:
        create_table(node, "source", True)
        create_table(node, "destination", True)

    replica1.query("INSERT INTO source VALUES ('2010-03-02 02:01:01')")
    replica1.query("SYSTEM SYNC REPLICA source")
    replica1.query("SYSTEM SYNC REPLICA destination")
    replica1.query(
        f"ALTER TABLE source MOVE PARTITION ID '20100302' TO TABLE destination"
    )

    assert_eq_with_retry(
        replica1, f"SELECT * FROM destination", "2010-03-02 02:01:01\n"
    )
    assert_eq_with_retry(
        replica1,
        f"SELECT * FROM destination",
        replica2.query(f"SELECT * FROM destination"),
    )


def test_both_replicated_partitioned_to_unpartitioned(start_cluster):
    cleanup([replica1, replica2])

    def create_tables(nodes):
        for node in nodes:
            source_engine = (
                f"ReplicatedMergeTree('/clickhouse/tables/1/source', '{node.name}')"
            )
            node.query(
                """
                CREATE TABLE source(timestamp DateTime)
                ENGINE = {engine}
                ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp)
                SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1;
                """.format(
                    engine=source_engine,
                )
            )

            destination_engine = f"ReplicatedMergeTree('/clickhouse/tables/1/destination', '{node.name}')"
            node.query(
                """
                CREATE TABLE destination(timestamp DateTime)
                ENGINE = {engine}
                ORDER BY tuple() PARTITION BY tuple()
                SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, allow_experimental_alter_partition_with_different_key=1;
                """.format(
                    engine=destination_engine,
                )
            )

    create_tables([replica1, replica2])

    replica1.query("INSERT INTO source VALUES ('2010-03-02 02:01:01')")
    replica1.query("INSERT INTO source VALUES ('2010-03-03 02:01:01')")
    replica1.query("SYSTEM SYNC REPLICA source")
    replica1.query("SYSTEM SYNC REPLICA destination")

    replica1.query(
        f"ALTER TABLE source MOVE PARTITION ID '20100302' TO TABLE destination"
    )
    replica1.query(
        f"ALTER TABLE source MOVE PARTITION ID '20100303' TO TABLE destination"
    )

    assert_eq_with_retry(
        replica1,
        f"SELECT * FROM destination ORDER BY timestamp",
        "2010-03-02 02:01:01\n2010-03-03 02:01:01\n",
    )
    assert_eq_with_retry(
        replica1,
        f"SELECT * FROM destination ORDER BY timestamp",
        replica2.query(f"SELECT * FROM destination ORDER BY timestamp"),
    )


def test_both_replicated_different_exp_same_id(start_cluster):
    cleanup([replica1, replica2])

    def create_tables(nodes):
        for node in nodes:
            source_engine = (
                f"ReplicatedMergeTree('/clickhouse/tables/1/source', '{node.name}')"
            )
            node.query(
                """
                CREATE TABLE source(a UInt16,b UInt16,c UInt16,extra UInt64,Path String,Time DateTime,Value Float64,Timestamp Int64,sign Int8)
                ENGINE = {engine}
                ORDER BY tuple() PARTITION BY a % 3
                SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1;
                """.format(
                    engine=source_engine,
                )
            )

            destination_engine = f"ReplicatedMergeTree('/clickhouse/tables/1/destination', '{node.name}')"
            node.query(
                """
                CREATE TABLE destination(a UInt16,b UInt16,c UInt16,extra UInt64,Path String,Time DateTime,Value Float64,Timestamp Int64,sign Int8)
                ENGINE = {engine}
                ORDER BY tuple() PARTITION BY a
                SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, allow_experimental_alter_partition_with_different_key=1;
                """.format(
                    engine=destination_engine,
                )
            )

    create_tables([replica1, replica2])

    replica1.query(
        "INSERT INTO source (a, b, c, extra, sign) VALUES (1, 5, 9, 1000, 1)"
    )
    replica1.query(
        "INSERT INTO source (a, b, c, extra, sign) VALUES (2, 6, 10, 1000, 1)"
    )
    replica1.query("SYSTEM SYNC REPLICA source")
    replica1.query("SYSTEM SYNC REPLICA destination")

    replica1.query(f"ALTER TABLE source MOVE PARTITION 1 TO TABLE destination")
    replica1.query(f"ALTER TABLE source MOVE PARTITION 2 TO TABLE destination")

    assert_eq_with_retry(
        replica1,
        f"SELECT * FROM destination ORDER BY a",
        "1\t5\t9\t1000\t\t1970-01-01 00:00:00\t0\t0\t1\n2\t6\t10\t1000\t\t1970-01-01 00:00:00\t0\t0\t1\n",
    )
    assert_eq_with_retry(
        replica1,
        f"SELECT * FROM destination ORDER BY a",
        replica2.query(f"SELECT * FROM destination ORDER BY a"),
    )
