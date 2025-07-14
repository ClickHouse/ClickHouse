import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import check_flags_deleted, set_convert_flags

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

database_name = "modify_engine_with_mv"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=database_name, sql=query)


def create_tables():
    q(
        ch1,
        "CREATE TABLE hourly_data(`domain_name` String, `event_time` DateTime, `count_views` UInt64) ENGINE = MergeTree ORDER BY (domain_name, event_time)",
    )

    q(
        ch1,
        "CREATE TABLE monthly_aggregated_data\
            (`domain_name` String, `month` Date, `sumCountViews` AggregateFunction(sum, UInt64))\
            ENGINE = AggregatingMergeTree ORDER BY (domain_name, month)",
    )

    q(
        ch1,
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
                month",
    )

    q(
        ch1,
        "INSERT INTO hourly_data (domain_name, event_time, count_views)\
            VALUES ('clickhouse.com', '2019-01-01 10:00:00', 1),\
                ('clickhouse.com', '2019-02-02 00:00:00', 2),\
                ('clickhouse.com', '2019-02-01 00:00:00', 3),\
                ('clickhouse.com', '2020-01-01 00:00:00', 6)",
    )


def check_tables(converted):
    engine_prefix = ""
    if converted:
        engine_prefix = "Replicated"

    # Check engines
    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}'",
        ).strip()
        == f"hourly_data\t{engine_prefix}MergeTree\nmonthly_aggregated_data\t{engine_prefix}AggregatingMergeTree\nmonthly_aggregated_data_mv\tMaterializedView"
    )

    # Check values
    assert (
        q(
            ch1,
            "SELECT sumMerge(sumCountViews) as sumCountViews\
            FROM monthly_aggregated_data_mv",
        ).strip()
        == "12"
    )
    assert q(ch1, "SELECT count() FROM hourly_data").strip() == "4"

    if converted:
        # Insert new values to check if new dependencies are set correctly
        q(
            ch1,
            "INSERT INTO hourly_data (domain_name, event_time, count_views)\
                VALUES ('clickhouse.com', '2019-01-01 10:00:00', 1),\
                    ('clickhouse.com', '2019-02-02 00:00:00', 2),\
                    ('clickhouse.com', '2019-02-01 00:00:00', 3),\
                    ('clickhouse.com', '2020-01-01 00:00:00', 6)",
        )

        assert (
            q(
                ch1,
                "SELECT sumMerge(sumCountViews) as sumCountViews\
                FROM monthly_aggregated_data_mv",
            ).strip()
            == "24"
        )
        assert q(ch1, "SELECT count() FROM hourly_data").strip() == "8"


def test_modify_engine_on_restart_with_materialized_view(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    create_tables()

    check_tables(False)

    set_convert_flags(ch1, database_name, ["hourly_data", "monthly_aggregated_data"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["hourly_data", "monthly_aggregated_data"])

    check_tables(True)

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
