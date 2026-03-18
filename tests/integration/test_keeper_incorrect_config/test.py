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

LOCALHOST_WITH_REMOTE = """
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

        <hostname_checks_enabled>true</hostname_checks_enabled>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>localhost</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname>127.0.0.2</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
"""

MULTIPLE_LOCAL_WITH_REMOTE = """
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

        <hostname_checks_enabled>true</hostname_checks_enabled>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>127.0.0.1</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname>127.0.1.1</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>3</id>
                <hostname>127.0.0.2</hostname>
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

JUST_WRONG_CONFIG = """
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
                <id>2</id>
                <hostname>node2</hostname>
                <port>9234</port>
                <id>3</id>
                <hostname>node3</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
"""


def test_invalid_configs(started_cluster):
    node1.stop_clickhouse()

    def assert_config_fails(config):
        node1.replace_config(
            "/etc/clickhouse-server/config.d/enable_keeper1.xml", config
        )
        with pytest.raises(Exception):
            node1.start_clickhouse(start_wait_sec=10)

    assert_config_fails(DUPLICATE_ENDPOINT_CONFIG)
    assert_config_fails(DUPLICATE_ID_CONFIG)
    assert_config_fails(LOCALHOST_WITH_REMOTE)
    assert_config_fails(MULTIPLE_LOCAL_WITH_REMOTE)
    assert_config_fails(JUST_WRONG_CONFIG)

    node1.replace_config(
        "/etc/clickhouse-server/config.d/enable_keeper1.xml", NORMAL_CONFIG
    )
    node1.start_clickhouse()

    assert node1.query("SELECT 1") == "1\n"
