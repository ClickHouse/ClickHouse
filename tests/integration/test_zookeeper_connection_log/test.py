import pytest
import logging

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(
    __file__, zookeeper_config_path="configs/zookeeper_config.xml"
)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    stay_alive=True,
    main_configs=[
        "configs/zookeeper_connection_log.xml",
        "configs/auxiliary_zookeepers.xml",
        "configs/config_reloader.xml",
    ],
    keeper_randomize_feature_flags=False,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
    keeper_randomize_feature_flags=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_zookeeper_connection_log(started_cluster):
    node1.query("DROP TABLE IF EXISTS simple SYNC")
    node1.query("DROP TABLE IF EXISTS simple2 SYNC")

    test_start_time = node1.query("SELECT now64()").strip()
    logging.debug(f"Test start time is {test_start_time}")

    # Let's restart ClickHouse to make sure there are log entries for initialization.
    # By restarting we can also make sure there won't be any config reloads in case of repeated runs.
    # The previous run would revert the config, but we need to reset the state of config.
    node1.restart_clickhouse()

    node1.query(
        "CREATE TABLE simple (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', 'node') ORDER BY tuple() PARTITION BY date;"
    )
    node1.query("INSERT INTO simple VALUES ('2020-08-27', 1)")
    node1.query("INSERT INTO simple VALUES ('2020-08-28', 1)")
    node1.query("INSERT INTO simple VALUES ('2020-08-29', 1)")

    node1.query(
        "CREATE TABLE simple2 (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/1/simple', 'node') ORDER BY tuple() PARTITION BY date;"
    )

    node1.query(
        "ALTER TABLE simple2 FETCH PARTITION '2020-08-27' FROM 'zk_conn_log_test_2:/clickhouse/tables/0/simple';"
    )

    node1.query(
        "ALTER TABLE simple2 FETCH PARTITION '2020-08-28' FROM 'zk_conn_log_test_3:/clickhouse/tables/0/simple';"
    )

    new_auxiliary_config = """<clickhouse>
    <auxiliary_zookeepers>
        <zk_conn_log_test_2>
            <node index="1">
                <host>zoo3</host>
                <port>2181</port>
            </node>
        </zk_conn_log_test_2>
        <zk_conn_log_test_4>
            <node index="1">
                <host>zoo2</host>
                <port>2181</port>
            </node>
        </zk_conn_log_test_4>
    </auxiliary_zookeepers>
</clickhouse>"""

    new_config = """<clickhouse>
    <zookeeper>
        <node index="1">
            <host>zoo2</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>15000</session_timeout_ms>
    </zookeeper>
</clickhouse>"""

    with node1.with_replace_config(
        "/etc/clickhouse-server/conf.d/zookeeper_config.xml", new_config
    ):
        with node1.with_replace_config(
            "/etc/clickhouse-server/config.d/auxiliary_zookeepers.xml",
            new_auxiliary_config,
        ):

            node1.query("SYSTEM RELOAD CONFIG")

            def check_8_rows(res):
                logging.debug(
                    f"Checking for 8 rows in zookeeper_connection_log, got: {res}"
                )
                return res == "8\n"

            node1.query_with_retry(
                f"SELECT count() FROM system.zookeeper_connection_log WHERE event_time_microseconds >= '{test_start_time}'",
                check_callback=check_8_rows,
            )

            node1.query(
                "ALTER TABLE simple2 FETCH PARTITION '2020-08-29' FROM 'zk_conn_log_test_4:/clickhouse/tables/0/simple';"
            )

            node1.query("SYSTEM FLUSH LOGS")

            logging.debug(
                node1.query(
                    """SELECT event_time_microseconds, hostname, type, name, host, port, index, keeper_api_version, enabled_feature_flags, reason
                               FROM system.zookeeper_connection_log  ORDER BY event_time_microseconds"""
                )
            )
            expected = TSV(
                """node1	Connected	default	zoo1	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Initialization
node1	Connected	zk_conn_log_test_2	zoo2	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Initialization
node1	Connected	zk_conn_log_test_3	zoo3	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Initialization
node1	Disconnected	default	zoo1	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Config changed
node1	Connected	default	zoo2	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Config changed
node1	Disconnected	zk_conn_log_test_2	zoo2	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Config changed
node1	Connected	zk_conn_log_test_2	zoo3	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Config changed
node1	Disconnected	zk_conn_log_test_3	zoo3	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Removed from config
node1	Connected	zk_conn_log_test_4	zoo2	2181	0	0	['FILTERED_LIST','MULTI_READ','CHECK_NOT_EXISTS','CREATE_IF_NOT_EXISTS','REMOVE_RECURSIVE','MULTI_WATCHES']	Initialization"""
            )

            assert (
                TSV(
                    node1.query(
                        f"""SELECT hostname, type, name, host, port, index, keeper_api_version, enabled_feature_flags, reason
                               FROM system.zookeeper_connection_log
                               WHERE event_time_microseconds >= '{test_start_time}'
                               ORDER BY event_time_microseconds"""
                    )
                )
                == expected
            )
            assert (
                int(
                    node1.query(
                        "SELECT max(event_per_client_id) FROM (SELECT client_id, count() AS event_per_client_id FROM system.zookeeper_connection_log GROUP BY client_id)"
                    )
                )
                == 2
            )


def test_connect_on_reload(started_cluster, request):
    node2.replace_in_config(
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
        "use_cluster>1",
        "use_cluster>0",
    )
    node2.restart_clickhouse()
    node2.replace_in_config(
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
        "use_cluster>0",
        "use_cluster>1",
    )
    node2.query("SYSTEM RELOAD CONFIG")
    node2.query("SELECT 1")
