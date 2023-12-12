import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node2"},
)

database_name = "modify_engine"

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def q(node, query):
    return node.query(
                    database=database_name,
                    sql=query
                    )

def create_tables():
    # MergeTree table that will be converted
    q(
        ch1,
        "CREATE TABLE foo ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;"
    )

    q(
        ch1,
        "INSERT INTO foo SELECT number, today(), '' FROM numbers(1e6);"
    )
    q(
        ch1,
        "INSERT INTO foo SELECT number, today()-60, '' FROM numbers(1e5);"
    )

    # ReplacingMergeTree table that will be converted to check unusual engine kinds
    q(
        ch1,
        "CREATE TABLE replacing ( A Int64, D Date, S String ) ENGINE ReplacingMergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;"
    )

    q(
        ch1,
        "INSERT INTO replacing SELECT number, today(), '' FROM numbers(1e6);"
    )
    q(
        ch1,
        "INSERT INTO replacing SELECT number, today()-60, '' FROM numbers(1e5);"
    )

    # MergeTree table that will not be converted
    q(
        ch1,
        "CREATE TABLE bar ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;"
    )

    # Not MergeTree table
    q(
        ch1,
        "CREATE TABLE bar2 ( A Int64, D Date, S String ) ENGINE Log;"
    )

def check_tables_not_converted():
    # Check tables exists
    assert q(
        ch1,
        "SHOW TABLES",
    ).strip() == "bar\nbar2\nfoo\nreplacing"

    # Check engines
    assert q(
        ch1,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name LIKE 'foo%' OR name LIKE 'replacing%')",
    ).strip() == "foo\tMergeTree\nreplacing\tReplacingMergeTree"
    assert q(
        ch1,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name LIKE 'bar%')",
    ).strip() == "bar\tMergeTree\nbar2\tLog"

    # Check values
    for table in ["foo", "replacing"]:
        assert q(
            ch1,
            f"SELECT count() FROM {table}",
        ).strip() == "1100000"

def check_tables_converted():
    # Check tables exists
    assert q(
        ch1,
        "SHOW TABLES",
    ).strip() == "bar\nbar2\nfoo\nfoo_temp\nreplacing\nreplacing_temp"

    # Check engines
    assert q(
        ch1,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name LIKE 'foo%' OR name LIKE 'replacing%')",
    ).strip() == "foo\tReplicatedMergeTree\nfoo_temp\tMergeTree\nreplacing\tReplicatedReplacingMergeTree\nreplacing_temp\tReplacingMergeTree"
    assert q(
        ch1,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND (name LIKE 'bar%')",
    ).strip() == "bar\tMergeTree\nbar2\tLog"

    # Check values
    for table in ["foo", "replacing"]:
        assert q(
            ch1,
            f"SELECT count() FROM {table}",
        ).strip() == "1100000"
        assert q(
            ch1,
            f"SELECT count() FROM {table}_temp",
        ).strip() == "1100000"

def set_convert_flags():

    # Set convert flag on actually convertable tables
    for table in ["foo", "replacing"]:
        ch1.exec_in_container(
            ["bash", "-c", f"mkdir /var/lib/clickhouse/data/{database_name}/{table}/flags"]
        )
        ch1.exec_in_container(
            ["bash", "-c", f"touch /var/lib/clickhouse/data/{database_name}/{table}/flags/convert_to_replicated"]
        )

    # Set flag to not MergeTree table to check that nothing happens
    ch1.exec_in_container(
        ["bash", "-c", f"mkdir /var/lib/clickhouse/data/{database_name}/bar2/flags"]
    )
    ch1.exec_in_container(
        ["bash", "-c", f"touch /var/lib/clickhouse/data/{database_name}/bar2/flags/convert_to_replicated"]
    )

def check_replica_added():
    # Add replica to check if zookeeper path is correct and consistent with table uuid

    uuid = q(
        ch1,
        f"SELECT uuid FROM system.tables WHERE table = 'foo' AND database = '{database_name}'"
    ).strip()

    q(
        ch2,
        f"CREATE TABLE foo ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/tables/{uuid}/{{shard}}', '{{replica}}') PARTITION BY toYYYYMM(D) ORDER BY A"
    )
    
    ch2.query(database=database_name, sql="SYSTEM SYNC REPLICA foo", timeout=20)

    # Check values
    assert q(
        ch2,
        f"SELECT count() FROM foo",
    ).strip() == "1100000"

def test_modify_engine_on_restart(started_cluster):
    ch1.query("CREATE DATABASE " + database_name + " ON CLUSTER cluster")
    assert q(
        ch1,
        "SHOW TABLES"
    ).strip() == ""

    create_tables()

    check_tables_not_converted()

    ch1.restart_clickhouse()

    check_tables_not_converted()

    set_convert_flags()

    ch1.restart_clickhouse()

    check_tables_converted()

    check_replica_added()
