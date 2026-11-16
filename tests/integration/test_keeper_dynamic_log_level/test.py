import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/keeper_config.xml",
        "configs/logger.xml",
    ],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def count_trace_messages():
    return int(
        node.exec_in_container(
            [
                "bash",
                "-c",
                "grep '<Trace>' /var/log/clickhouse-server/clickhouse-server.log | wc -l",
            ],
            privileged=True,
            user="root",
        )
    )


def query_and_count_trace_messages():
    node.query(
        "SELECT * FROM system.zookeeper SETTINGS allow_unrestricted_reads_from_keeper = 'true'"
    )
    return count_trace_messages()


def test_adjust_log_level(start_cluster):
    assert count_trace_messages() == 0

    # Adjust log level.
    node.exec_in_container(
        [
            "bash",
            "-c",
            """echo "
<clickhouse>
    <logger>
        <level>trace</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
        <errorlog_level>error</errorlog_level>
        <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
        <size>200M</size>
        <count>10</count>
    </logger>
</clickhouse>
            " > /etc/clickhouse-server/config.d/logger.xml
            """,
        ]
    )

    # `ConfigReloader` waits for `config_reload_interval_ms` (2 seconds by default) and only then
    # re-reads the whole configuration, so the new level takes effect later than that interval
    # alone suggests - under a sanitizer build on a loaded machine, much later. Wait for the level
    # to be applied instead of sleeping for a fixed time, and issue the query on every attempt,
    # because the query is what produces the `Trace` messages we are looking for.
    wait_condition(
        query_and_count_trace_messages,
        lambda count: count >= 1,
        max_attempts=60,
        delay=1,
    )
