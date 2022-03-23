#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


DUPLICATE_ID_CONFIG = """
<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

        <coordination_settings>
            <operation_timeout_ms>5000</operation_timeout_ms>
            <session_timeout_ms>10000</session_timeout_ms>
            <raft_logs_level>trace</raft_logs_level>
        </coordination_settings>

        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>node1</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>1</id>
                <hostname>node2</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
"""

DUPLICATE_ENDPOINT_CONFIG = """
<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

        <coordination_settings>
            <operation_timeout_ms>5000</operation_timeout_ms>
            <session_timeout_ms>10000</session_timeout_ms>
            <raft_logs_level>trace</raft_logs_level>
        </coordination_settings>

        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>node1</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname>node1</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
"""

NORMAL_CONFIG = """
<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>

        <coordination_settings>
            <operation_timeout_ms>5000</operation_timeout_ms>
            <session_timeout_ms>10000</session_timeout_ms>
            <raft_logs_level>trace</raft_logs_level>
        </coordination_settings>

        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>node1</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
"""


def test_duplicate_endpoint(started_cluster):
    node1.stop_clickhouse()
    node1.replace_config(
        "/etc/clickhouse-server/config.d/enable_keeper1.xml", DUPLICATE_ENDPOINT_CONFIG
    )

    with pytest.raises(Exception):
        node1.start_clickhouse(start_wait_sec=10)

    node1.replace_config(
        "/etc/clickhouse-server/config.d/enable_keeper1.xml", DUPLICATE_ID_CONFIG
    )
    with pytest.raises(Exception):
        node1.start_clickhouse(start_wait_sec=10)

    node1.replace_config(
        "/etc/clickhouse-server/config.d/enable_keeper1.xml", NORMAL_CONFIG
    )
    node1.start_clickhouse()

    assert node1.query("SELECT 1") == "1\n"
