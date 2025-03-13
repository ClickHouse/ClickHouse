import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
NODES = {"node_" + str(i): None for i in (1, 2, 3)}

NODES["node"] = None

# Sleep time in milliseconds.
sleep_time = 30000


@pytest.fixture(scope="module")
def started_cluster():
    NODES["node"] = cluster.add_instance(
        "node",
        stay_alive=True,
        main_configs=["configs/remote_servers.xml", "configs/logger.xml"],
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
                "INSERT INTO test_hedged select number, toDate(number) from numbers(100);"
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
            <sleep_after_receiving_query_ms>{sleep_after_receiving_query_ms}</sleep_after_receiving_query_ms>
        </default>
    </profiles>
</clickhouse>"""


def check_query(expected_replica, receive_timeout=300):
    NODES["node"].restart_clickhouse()

    # Without hedged requests select query will last more than 30 seconds,
    # with hedged requests it will last just around 1-2 second

    start = time.time()
    result = NODES["node"].query(
        "SELECT hostName(), id FROM distributed ORDER BY id LIMIT 1 SETTINGS receive_timeout={}".format(
            receive_timeout
        )
    )
    query_time = time.time() - start

    assert TSV(result) == TSV(expected_replica + "\t0")

    print("Query time:", query_time)
    assert query_time < 10


def check_settings(
    node_name,
    sleep_in_send_tables_status_ms,
    sleep_in_send_data_ms,
    sleep_after_receiving_query_ms,
):
    attempts = 0
    while attempts < 1000:
        setting1 = NODES[node_name].http_query(
            "SELECT value FROM system.settings WHERE name='sleep_in_send_tables_status_ms'"
        )
        setting2 = NODES[node_name].http_query(
            "SELECT value FROM system.settings WHERE name='sleep_in_send_data_ms'"
        )
        setting3 = NODES[node_name].http_query(
            "SELECT value FROM system.settings WHERE name='sleep_after_receiving_query_ms'"
        )

        if (
            int(setting1) == sleep_in_send_tables_status_ms
            and int(setting2) == sleep_in_send_data_ms
            and int(setting3) == sleep_after_receiving_query_ms
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


def check_if_query_sending_was_suspended():
    result = NODES["node"].query(
        "SELECT value FROM system.events WHERE event='SuspendSendingQueryToShard'"
    )

    return len(result) != 0 and int(result) >= 1


def check_if_query_sending_was_not_suspended():
    result = NODES["node"].query(
        "SELECT value FROM system.events WHERE event='SuspendSendingQueryToShard'"
    )

    assert result == ""


def update_configs(
    node_1_sleep_in_send_tables_status=0,
    node_1_sleep_in_send_data=0,
    node_1_sleep_after_receiving_query=0,
    node_2_sleep_in_send_tables_status=0,
    node_2_sleep_in_send_data=0,
    node_2_sleep_after_receiving_query=0,
    node_3_sleep_in_send_tables_status=0,
    node_3_sleep_in_send_data=0,
    node_3_sleep_after_receiving_query=0,
):
    NODES["node_1"].replace_config(
        "/etc/clickhouse-server/users.d/users1.xml",
        config.format(
            sleep_in_send_tables_status_ms=node_1_sleep_in_send_tables_status,
            sleep_in_send_data_ms=node_1_sleep_in_send_data,
            sleep_after_receiving_query_ms=node_1_sleep_after_receiving_query,
        ),
    )

    NODES["node_2"].replace_config(
        "/etc/clickhouse-server/users.d/users1.xml",
        config.format(
            sleep_in_send_tables_status_ms=node_2_sleep_in_send_tables_status,
            sleep_in_send_data_ms=node_2_sleep_in_send_data,
            sleep_after_receiving_query_ms=node_2_sleep_after_receiving_query,
        ),
    )

    NODES["node_3"].replace_config(
        "/etc/clickhouse-server/users.d/users1.xml",
        config.format(
            sleep_in_send_tables_status_ms=node_3_sleep_in_send_tables_status,
            sleep_in_send_data_ms=node_3_sleep_in_send_data,
            sleep_after_receiving_query_ms=node_3_sleep_after_receiving_query,
        ),
    )

    check_settings(
        "node_1",
        node_1_sleep_in_send_tables_status,
        node_1_sleep_in_send_data,
        node_1_sleep_after_receiving_query,
    )
    check_settings(
        "node_2",
        node_2_sleep_in_send_tables_status,
        node_2_sleep_in_send_data,
        node_2_sleep_after_receiving_query,
    )
    check_settings(
        "node_3",
        node_3_sleep_in_send_tables_status,
        node_3_sleep_in_send_data,
        node_3_sleep_after_receiving_query,
    )


def test_stuck_replica(started_cluster):
    update_configs()

    cluster.pause_container("node_1")

    check_query(expected_replica="node_2")
    check_changing_replica_events(1)

    result = NODES["node"].query(
        "SELECT slowdowns_count FROM system.clusters WHERE cluster='test_cluster' and host_name='node_1'"
    )

    assert TSV(result) == TSV("1")

    result = NODES["node"].query(
        "SELECT hostName(), id FROM distributed ORDER BY id LIMIT 1"
    )

    assert TSV(result) == TSV("node_2\t0")

    # Check that we didn't choose node_1 first again and slowdowns_count didn't increase.
    result = NODES["node"].query(
        "SELECT slowdowns_count FROM system.clusters WHERE cluster='test_cluster' and host_name='node_1'"
    )

    assert TSV(result) == TSV("1")

    cluster.unpause_container("node_1")


def test_long_query(started_cluster):
    update_configs()

    # Restart to reset pool states.
    NODES["node"].restart_clickhouse()

    result = NODES["node"].query(
        "select hostName(), max(id + sleep(1.5)) from distributed settings max_block_size = 1, max_threads = 1, max_distributed_connections = 1;"
    )
    assert TSV(result) == TSV("node_1\t99")

    NODES["node"].query(
        "INSERT INTO distributed select number, toDate(number) from numbers(100);"
    )


def test_send_table_status_sleep(started_cluster):
    update_configs(node_1_sleep_in_send_tables_status=sleep_time)
    check_query(expected_replica="node_2")
    check_changing_replica_events(1)


def test_send_table_status_sleep2(started_cluster):
    update_configs(
        node_1_sleep_in_send_tables_status=sleep_time,
        node_2_sleep_in_send_tables_status=sleep_time,
    )
    check_query(expected_replica="node_3")
    check_changing_replica_events(2)


def test_send_data(started_cluster):
    update_configs(node_1_sleep_in_send_data=sleep_time)
    check_query(expected_replica="node_2")
    check_changing_replica_events(1)


def test_send_data2(started_cluster):
    update_configs(
        node_1_sleep_in_send_data=sleep_time, node_2_sleep_in_send_data=sleep_time
    )
    check_query(expected_replica="node_3")
    check_changing_replica_events(2)


def test_combination1(started_cluster):
    update_configs(
        node_1_sleep_in_send_tables_status=sleep_time,
        node_2_sleep_in_send_data=sleep_time,
    )
    check_query(expected_replica="node_3")
    check_changing_replica_events(2)


def test_combination2(started_cluster):
    update_configs(
        node_1_sleep_in_send_data=sleep_time,
        node_2_sleep_in_send_tables_status=sleep_time,
    )
    check_query(expected_replica="node_3")
    check_changing_replica_events(2)


def test_combination3(started_cluster):
    update_configs(
        node_1_sleep_in_send_data=sleep_time,
        node_2_sleep_in_send_tables_status=1000,
        node_3_sleep_in_send_data=sleep_time,
    )
    check_query(expected_replica="node_2")
    check_changing_replica_events(3)


def test_combination4(started_cluster):
    update_configs(
        node_1_sleep_in_send_tables_status=1000,
        node_1_sleep_in_send_data=sleep_time,
        node_2_sleep_in_send_tables_status=1000,
        node_3_sleep_in_send_tables_status=1000,
    )
    check_query(expected_replica="node_2")
    check_changing_replica_events(4)


def test_receive_timeout1(started_cluster):
    # Check the situation when first two replicas get receive timeout
    # in establishing connection, but the third replica is ok.
    update_configs(
        node_1_sleep_in_send_tables_status=3000,
        node_2_sleep_in_send_tables_status=3000,
        node_3_sleep_in_send_data=1000,
    )
    check_query(expected_replica="node_3", receive_timeout=2)
    check_changing_replica_events(2)


def test_receive_timeout2(started_cluster):
    # Check the situation when first replica get receive timeout
    # in packet receiving but there are replicas in process of
    # connection establishing.
    update_configs(
        node_1_sleep_in_send_data=5000,
        node_2_sleep_in_send_tables_status=2000,
        node_3_sleep_in_send_tables_status=2000,
    )
    check_query(expected_replica="node_2", receive_timeout=3)
    check_changing_replica_events(3)


def test_initial_receive_timeout(started_cluster):
    # Check the situation when replicas don't respond after
    # receiving query (so, no packets were send to initiator)
    update_configs(
        node_1_sleep_after_receiving_query=20000,
        node_2_sleep_after_receiving_query=20000,
        node_3_sleep_after_receiving_query=20000,
    )

    NODES["node"].restart_clickhouse()

    result = NODES["node"].query_and_get_error(
        "SELECT hostName(), id FROM distributed ORDER BY id LIMIT 1 SETTINGS receive_timeout=3"
    )

    assert "SOCKET_TIMEOUT" in result


def test_async_connect(started_cluster):
    update_configs()

    NODES["node"].restart_clickhouse()

    NODES["node"].query("DROP TABLE IF EXISTS distributed_connect")

    NODES["node"].query(
        """CREATE TABLE distributed_connect (id UInt32, date Date) ENGINE =
        Distributed('test_cluster_connect', 'default', 'test_hedged')"""
    )

    NODES["node"].query(
        "SELECT hostName(), id FROM distributed_connect ORDER BY id LIMIT 1 SETTINGS prefer_localhost_replica = 0, connect_timeout_with_failover_ms=5000, async_query_sending_for_remote=0, max_threads=1, max_distributed_connections=1"
    )
    check_changing_replica_events(2)
    check_if_query_sending_was_not_suspended()

    # Restart server to reset connection pool state
    NODES["node"].restart_clickhouse()

    attempt = 0
    while attempt < 100:
        NODES["node"].query(
            "SELECT hostName(), id FROM distributed_connect ORDER BY id LIMIT 1 SETTINGS prefer_localhost_replica = 0, connect_timeout_with_failover_ms=5000, async_query_sending_for_remote=1, max_threads=1, max_distributed_connections=1"
        )

        check_changing_replica_events(2)
        if check_if_query_sending_was_suspended():
            break

        attempt += 1

    assert attempt < 100

    NODES["node"].query("DROP TABLE distributed_connect")


def test_async_query_sending(started_cluster):
    update_configs(
        node_1_sleep_after_receiving_query=5000,
        node_2_sleep_after_receiving_query=5000,
        node_3_sleep_after_receiving_query=5000,
    )

    NODES["node"].restart_clickhouse()

    NODES["node"].query("DROP TABLE IF EXISTS distributed_query_sending")

    NODES["node"].query(
        """CREATE TABLE distributed_query_sending (id UInt32, date Date) ENGINE =
        Distributed('test_cluster_three_shards', 'default', 'test_hedged')"""
    )

    # Create big enough temporary table
    NODES["node"].query("DROP TABLE IF EXISTS tmp")
    NODES["node"].query(
        "CREATE TEMPORARY TABLE tmp (number UInt64, s String) "
        "as select number, randomString(number % 1000) from numbers(10000000)"
    )

    NODES["node"].query(
        "SELECT hostName(), id FROM distributed_query_sending ORDER BY id LIMIT 1 SETTINGS"
        " prefer_localhost_replica = 0, async_query_sending_for_remote=0, max_threads = 1, max_distributed_connections=1"
    )
    check_if_query_sending_was_not_suspended()

    attempt = 0
    while attempt < 100:
        NODES["node"].query(
            "SELECT hostName(), id FROM distributed_query_sending ORDER BY id LIMIT 1 SETTINGS"
            " prefer_localhost_replica = 0, async_query_sending_for_remote=1, max_threads = 1, max_distributed_connections=1"
        )

        if check_if_query_sending_was_suspended():
            break

        attempt += 1

    assert attempt < 100
    NODES["node"].query("DROP TABLE distributed_query_sending")
