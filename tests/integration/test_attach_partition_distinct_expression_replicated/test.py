import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

replica1 = cluster.add_instance(
    "replica1", with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "replica1", "shard": "s1"},
)
replica2 = cluster.add_instance(
    "replica2", with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "replica2", "shard": "s1"},
)

replica3 = cluster.add_instance(
    "replica3", with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "replica3", "shard": "s1"},
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
        node.query("DROP TABLE IF EXISTS source ON CLUSTER test_cluster SYNC")
        node.query("DROP TABLE IF EXISTS destination ON CLUSTER test_cluster SYNC")


def create_table(node, table_name, replicated):
    engine = (
        f"ReplicatedMergeTree"
        if replicated
        else "MergeTree()"
    )
    partition_expression = (
        "toYYYYMMDD(timestamp)" if table_name == "source" else "toYYYYMM(timestamp)"
    )
    node.query(
        """
        CREATE TABLE {table_name} ON CLUSTER test_cluster (timestamp DateTime)
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

    create_table(replica1, "source", True)
    create_table(replica1, "destination", True)

    replica1.query("INSERT INTO source VALUES ('2010-03-02 02:01:01')")
    replica1.query("SYSTEM SYNC REPLICA source")
    replica1.query(
        f"ALTER TABLE destination ATTACH PARTITION ID '20100302' FROM source"
    )

    assert_eq_with_retry(
        replica1, f"SELECT * FROM destination", "2010-03-02 02:01:01\n"
    )
    assert_eq_with_retry(
        replica1,
        f"SELECT * FROM destination",
        replica2.query(f"SELECT * FROM destination"),
    )

    cleanup([replica1, replica2])


def test_only_destination_replicated(start_cluster):
    cleanup([replica1, replica2])

    create_table(replica1, "source", False)
    create_table(replica1, "destination", True)

    replica1.query("INSERT INTO source VALUES ('2010-03-02 02:01:01')")
    replica1.query(
        f"ALTER TABLE destination ATTACH PARTITION ID '20100302' FROM source"
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

    replica1.query(
        """
        CREATE TABLE source ON CLUSTER test_cluster (timestamp DateTime)
        ENGINE = ReplicatedMergeTree
        ORDER BY tuple() PARTITION BY toYYYYMMDD(timestamp)
        SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1;
        """
    )
    replica1.query(
        """
        CREATE TABLE destination ON CLUSTER test_cluster (timestamp DateTime)
        ENGINE = ReplicatedMergeTree
        ORDER BY tuple() PARTITION BY tuple()
        SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, allow_experimental_alter_partition_with_different_key=1;
        """
    )

    replica1.query("INSERT INTO source VALUES ('2010-03-02 02:01:01')")
    replica1.query("INSERT INTO source VALUES ('2010-03-03 02:01:01')")
    replica1.query("SYSTEM SYNC REPLICA source")

    replica1.query(
        f"ALTER TABLE destination ATTACH PARTITION ID '20100302' FROM source"
    )
    replica1.query(
        f"ALTER TABLE destination ATTACH PARTITION ID '20100303' FROM source"
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

    replica1.query(
        """
        CREATE TABLE source ON CLUSTER test_cluster (a UInt16,b UInt16,c UInt16,extra UInt64,Path String,Time DateTime,Value Float64,Timestamp Int64,sign Int8)
        ENGINE = ReplicatedMergeTree
        ORDER BY tuple() PARTITION BY a % 3
        SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1;
        """
    )

    replica1.query(
        """
        CREATE TABLE destination ON CLUSTER test_cluster (a UInt16,b UInt16,c UInt16,extra UInt64,Path String,Time DateTime,Value Float64,Timestamp Int64,sign Int8)
        ENGINE = ReplicatedMergeTree
        ORDER BY tuple() PARTITION BY a
        SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, allow_experimental_alter_partition_with_different_key=1;
        """
    )

    replica1.query(
        "INSERT INTO source (a, b, c, extra, sign) VALUES (1, 5, 9, 1000, 1)"
    )
    replica1.query(
        "INSERT INTO source (a, b, c, extra, sign) VALUES (2, 6, 10, 1000, 1)"
    )
    replica1.query("SYSTEM SYNC REPLICA source")

    replica1.query(f"ALTER TABLE destination ATTACH PARTITION 1 FROM source")
    replica1.query(f"ALTER TABLE destination ATTACH PARTITION 2 FROM source")

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
