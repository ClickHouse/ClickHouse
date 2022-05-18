import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

NODES = {"node_" + str(i): None for i in (1, 2, 3, 4)}
NODES["node"] = None

# Cleep time in milliseconds.
sleep_time = 30000


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    NODES["node"] = cluster.add_instance(
        "node",
        stay_alive=True,
        main_configs=["configs/remote_servers.xml"],
        user_configs=["configs/users.xml"],
    )

    for name in NODES:
        if name != "node":
            NODES[name] = cluster.add_instance(
                name, user_configs=["configs/users1.xml"]
            )

    try:
        cluster.start()

        for node_id, node in list(NODES.items()):
            node.query(
                """CREATE TABLE test_hedged (id UInt32, date Date) ENGINE =
            MergeTree()  ORDER BY id PARTITION BY toYYYYMM(date)"""
            )

            node.query(
                "INSERT INTO test_hedged SELECT number, toDateTime(number) FROM numbers(100)"
            )

        NODES["node"].query(
            """CREATE TABLE distributed (id UInt32, date Date) ENGINE =
            Distributed('test_cluster', 'default', 'test_hedged')"""
        )

        yield cluster

    finally:
        cluster.shutdown()


config = """<clickhouse>
    <profiles>
        <default>
            <sleep_in_send_tables_status_ms>{sleep_in_send_tables_status_ms}</sleep_in_send_tables_status_ms>
            <sleep_in_send_data_ms>{sleep_in_send_data_ms}</sleep_in_send_data_ms>
        </default>
    </profiles>
</clickhouse>"""


QUERY_1 = "SELECT count() FROM distributed"
QUERY_2 = "SELECT * FROM distributed"


def check_query(query=QUERY_1):
    NODES["node"].restart_clickhouse()

    # Without hedged requests select query will last more than 30 seconds,
    # with hedged requests it will last just around 1-2 second

    start = time.time()
    NODES["node"].query(query)
    query_time = time.time() - start
    print("Query time:", query_time)

    assert query_time < 5


def check_settings(node_name, sleep_in_send_tables_status_ms, sleep_in_send_data_ms):
    attempts = 0
    while attempts < 1000:
        setting1 = NODES[node_name].http_query(
            "SELECT value FROM system.settings WHERE name='sleep_in_send_tables_status_ms'"
        )
        setting2 = NODES[node_name].http_query(
            "SELECT value FROM system.settings WHERE name='sleep_in_send_data_ms'"
        )
        if (
            int(setting1) == sleep_in_send_tables_status_ms
            and int(setting2) == sleep_in_send_data_ms
        ):
            return
        time.sleep(0.1)
        attempts += 1

    assert attempts < 1000


def check_changing_replica_events(expected_count):
    result = NODES["node"].query(
        "SELECT value FROM system.events WHERE event='HedgedRequestsChangeReplica'"
    )

    # If server load is high we can see more than expected
    # replica change events, but never less than expected
    assert int(result) >= expected_count


def update_configs(
    node_1_sleep_in_send_tables_status=0,
    node_1_sleep_in_send_data=0,
    node_2_sleep_in_send_tables_status=0,
    node_2_sleep_in_send_data=0,
    node_3_sleep_in_send_tables_status=0,
    node_3_sleep_in_send_data=0,
    node_4_sleep_in_send_tables_status=0,
    node_4_sleep_in_send_data=0,
):
    NODES["node_1"].replace_config(
        "/etc/clickhouse-server/users.d/users1.xml",
        config.format(
            sleep_in_send_tables_status_ms=node_1_sleep_in_send_tables_status,
            sleep_in_send_data_ms=node_1_sleep_in_send_data,
        ),
    )

    NODES["node_2"].replace_config(
        "/etc/clickhouse-server/users.d/users1.xml",
        config.format(
            sleep_in_send_tables_status_ms=node_2_sleep_in_send_tables_status,
            sleep_in_send_data_ms=node_2_sleep_in_send_data,
        ),
    )

    NODES["node_3"].replace_config(
        "/etc/clickhouse-server/users.d/users1.xml",
        config.format(
            sleep_in_send_tables_status_ms=node_3_sleep_in_send_tables_status,
            sleep_in_send_data_ms=node_3_sleep_in_send_data,
        ),
    )

    NODES["node_4"].replace_config(
        "/etc/clickhouse-server/users.d/users1.xml",
        config.format(
            sleep_in_send_tables_status_ms=node_4_sleep_in_send_tables_status,
            sleep_in_send_data_ms=node_4_sleep_in_send_data,
        ),
    )

    check_settings(
        "node_1", node_1_sleep_in_send_tables_status, node_1_sleep_in_send_data
    )
    check_settings(
        "node_2", node_2_sleep_in_send_tables_status, node_2_sleep_in_send_data
    )
    check_settings(
        "node_3", node_3_sleep_in_send_tables_status, node_3_sleep_in_send_data
    )
    check_settings(
        "node_4", node_4_sleep_in_send_tables_status, node_4_sleep_in_send_data
    )


def test_send_table_status_sleep(started_cluster):
    update_configs(
        node_1_sleep_in_send_tables_status=sleep_time,
        node_2_sleep_in_send_tables_status=sleep_time,
    )
    check_query()
    check_changing_replica_events(2)


def test_send_data(started_cluster):
    update_configs(
        node_1_sleep_in_send_data=sleep_time, node_2_sleep_in_send_data=sleep_time
    )
    check_query()
    check_changing_replica_events(2)


def test_combination1(started_cluster):
    update_configs(
        node_1_sleep_in_send_tables_status=1000,
        node_2_sleep_in_send_tables_status=1000,
        node_3_sleep_in_send_data=sleep_time,
    )
    check_query()
    check_changing_replica_events(3)


def test_combination2(started_cluster):
    update_configs(
        node_1_sleep_in_send_data=sleep_time,
        node_2_sleep_in_send_tables_status=1000,
        node_3_sleep_in_send_data=sleep_time,
        node_4_sleep_in_send_tables_status=1000,
    )
    check_query()
    check_changing_replica_events(4)


def test_query_with_no_data_to_sample(started_cluster):
    update_configs(
        node_1_sleep_in_send_data=sleep_time, node_2_sleep_in_send_data=sleep_time
    )

    # When there is no way to sample data, the whole query will be performed by
    # the first replica and the second replica will just send EndOfStream,
    # so we will change only the first replica here.
    check_query(query=QUERY_2)
    check_changing_replica_events(1)
