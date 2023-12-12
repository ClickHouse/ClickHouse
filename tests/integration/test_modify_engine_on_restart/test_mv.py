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

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def q(node, database, query):
    return node.query(
                    database=database,
                    sql=query
                    )

def create_tables(database_name):
    q(
        ch1,
        database_name,
        "CREATE TABLE hourly_data(`domain_name` String, `event_time` DateTime, `count_views` UInt64) ENGINE = MergeTree ORDER BY (domain_name, event_time)"
    )

    q(
        ch1,
        database_name,
        "CREATE TABLE monthly_aggregated_data\
            (`domain_name` String, `month` Date, `sumCountViews` AggregateFunction(sum, UInt64))\
            ENGINE = AggregatingMergeTree ORDER BY (domain_name, month)"
    )

    q(
        ch1,
        database_name,
        "CREATE MATERIALIZED VIEW monthly_aggregated_data_mv\
            TO monthly_aggregated_data\
            AS\
            SELECT\
                toDate(toStartOfMonth(event_time)) AS month,\
                domain_name,\
                sumState(count_views) AS sumCountViews\
            FROM hourly_data\
            GROUP BY\
                domain_name,\
                month"
    )

    q(
        ch1,
        database_name,
        "INSERT INTO hourly_data (domain_name, event_time, count_views)\
            VALUES ('clickhouse.com', '2019-01-01 10:00:00', 1),\
                ('clickhouse.com', '2019-02-02 00:00:00', 2),\
                ('clickhouse.com', '2019-02-01 00:00:00', 3),\
                ('clickhouse.com', '2020-01-01 00:00:00', 6)"
    )

def check_tables_not_converted(database_name):
    # Check engines
    assert q(
        ch1,
        database_name,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}'",
    ).strip() == "hourly_data\tMergeTree\nmonthly_aggregated_data\tAggregatingMergeTree\nmonthly_aggregated_data_mv\tMaterializedView"

    # Check values
    assert q(
        ch1,
        database_name,
        "SELECT sumMerge(sumCountViews) as sumCountViews\
            FROM monthly_aggregated_data_mv"
    ).strip() == "12"
    assert q(
        ch1,
        database_name,
        "SELECT count() FROM hourly_data"
    ).strip() == "4"

def check_tables_converted(database_name):
    # Check engines
    assert q(
        ch1,
        database_name,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND name NOT LIKE '%_temp'",
    ).strip() == "hourly_data\tReplicatedMergeTree\nmonthly_aggregated_data\tReplicatedAggregatingMergeTree\nmonthly_aggregated_data_mv\tMaterializedView"
    assert q(
        ch1,
        database_name,
        f"SELECT name, engine FROM system.tables WHERE database = '{database_name}' AND name LIKE '%_temp'",
    ).strip() == "hourly_data_temp\tMergeTree\nmonthly_aggregated_data_temp\tAggregatingMergeTree"

    # Check values
    assert q(
        ch1,
        database_name,
        "SELECT sumMerge(sumCountViews) as sumCountViews\
            FROM monthly_aggregated_data_mv"
    ).strip() == "12"
    assert q(
        ch1,
        database_name,
        "SELECT count() FROM hourly_data"
    ).strip() == "4"

    # Insert new values to check if new dependencies are set correctly
    q(
        ch1,
        database_name,
        "INSERT INTO hourly_data (domain_name, event_time, count_views)\
            VALUES ('clickhouse.com', '2019-01-01 10:00:00', 1),\
                ('clickhouse.com', '2019-02-02 00:00:00', 2),\
                ('clickhouse.com', '2019-02-01 00:00:00', 3),\
                ('clickhouse.com', '2020-01-01 00:00:00', 6)"
    )

    assert q(
        ch1,
        database_name,
        "SELECT sumMerge(sumCountViews) as sumCountViews\
            FROM monthly_aggregated_data_mv"
    ).strip() == "24"
    assert q(
        ch1,
        database_name,
        "SELECT count() FROM hourly_data"
    ).strip() == "8"

def set_convert_flags(database_name):

    for table in ["hourly_data", "monthly_aggregated_data"]:
        ch1.exec_in_container(
            ["bash", "-c", f"mkdir /var/lib/clickhouse/data/{database_name}/{table}/flags"]
        )
        ch1.exec_in_container(
            ["bash", "-c", f"touch /var/lib/clickhouse/data/{database_name}/{table}/flags/convert_to_replicated"]
        )

def test_modify_engine_on_restart_with_materialized_view(started_cluster):
    database_name = "modify_engine_with_mv"
    q(
        ch1,
        "default",
        f"CREATE DATABASE {database_name}",
    )
    assert q(
        ch1,
        database_name,
        "SHOW TABLES"
    ).strip() == ""

    create_tables(database_name)

    check_tables_not_converted(database_name)

    set_convert_flags(database_name)

    ch1.restart_clickhouse()

    check_tables_converted(database_name)
    