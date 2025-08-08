# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node_default", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_logs_comment():
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <query_log>
                <engine>ENGINE = MergeTree
                        PARTITION BY (event_date)
                        ORDER BY (event_time)
                        TTL event_date + INTERVAL 14 DAY DELETE
                        SETTINGS ttl_only_drop_parts=1
                        COMMENT 'test_comment'
                </engine>
                <partition_by remove='remove'/>
            </query_log>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/yyy-override-query_log.xml
        """,
        ]
    )
    node.restart_clickhouse()

    node.query("select 1")
    node.query("system flush logs")

    comment = node.query("SELECT comment FROM system.tables WHERE name = 'query_log'")
    assert comment == "test_comment\n"
