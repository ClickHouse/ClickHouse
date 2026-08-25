import time

import pytest

from helpers.cluster import ClickHouseCluster

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


def test_adjust_log_level(start_cluster):
    assert (
        int(
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
        == 0
    )

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
    # The config reloader applies the new logger settings asynchronously (it polls the config
    # every couple of seconds), so poll until trace logging becomes active instead of relying
    # on a fixed sleep, which is not enough on slow (e.g. sanitizer) runs.
    trace_lines = 0
    for _ in range(60):
        node.query(
            "SELECT * FROM system.zookeeper SETTINGS allow_unrestricted_reads_from_keeper = 'true'"
        )
        node.exec_in_container(
            [
                "bash",
                "-c",
                "sync",
            ],
            privileged=True,
            user="root",
        )
        trace_lines = int(
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
        if trace_lines >= 1:
            break
        time.sleep(1)
    assert trace_lines >= 1
