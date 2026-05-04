import os
import pytest
import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

STREAMING_SETTINGS = """
            <nuraft_streaming_mode>true</nuraft_streaming_mode>
            <nuraft_max_log_gap_in_stream>16</nuraft_max_log_gap_in_stream>
            <nuraft_max_bytes_in_flight_in_stream>4194304</nuraft_max_bytes_in_flight_in_stream>"""

RAFT_CONFIGURATION = """
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>node1</hostname>
                <port>9234</port>
                <can_become_leader>true</can_become_leader>
                <priority>3</priority>
            </server>
            <server>
                <id>2</id>
                <hostname>node2</hostname>
                <port>9234</port>
                <can_become_leader>true</can_become_leader>
                <start_as_follower>true</start_as_follower>
                <priority>2</priority>
            </server>
            <server>
                <id>3</id>
                <hostname>node3</hostname>
                <port>9234</port>
                <can_become_leader>true</can_become_leader>
                <start_as_follower>true</start_as_follower>
                <priority>1</priority>
            </server>
        </raft_configuration>"""


def _keeper_config(server_id):
    return f"""<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>{server_id}</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>5000</operation_timeout_ms>
            <session_timeout_ms>10000</session_timeout_ms>
            <raft_logs_level>trace</raft_logs_level>{STREAMING_SETTINGS}
        </coordination_settings>{RAFT_CONFIGURATION}
    </keeper_server>
</clickhouse>
"""


USE_KEEPER_CONFIG = """<clickhouse>
    <zookeeper>
        <node index="1"><host>node1</host><port>9181</port></node>
        <node index="2"><host>node2</host><port>9181</port></node>
        <node index="3"><host>node3</host><port>9181</port></node>
    </zookeeper>
</clickhouse>
"""

_configs_dir = os.path.join(os.path.dirname(__file__), "configs")
os.makedirs(_configs_dir, exist_ok=True)

for _i in range(1, 4):
    with open(os.path.join(_configs_dir, f"enable_keeper{_i}.xml"), "w") as _f:
        _f.write(_keeper_config(_i))

with open(os.path.join(_configs_dir, "use_keeper.xml"), "w") as _f:
    _f.write(USE_KEEPER_CONFIG)


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [node1, node2, node3])
        yield cluster
    finally:
        cluster.shutdown()


def test_streaming_mode_enabled_in_config(started_cluster):
    """Verify that nuraft_streaming_mode is reported as enabled via the conf 4LW command."""
    conf = keeper_utils.send_4lw_cmd(cluster, node1, "conf")
    assert "nuraft_streaming_mode=true" in conf, f"Streaming mode not found in conf output:\n{conf}"
    assert "nuraft_max_log_gap_in_stream=16" in conf
    assert "nuraft_max_bytes_in_flight_in_stream=4194304" in conf


def test_streaming_mode_logged_at_startup(started_cluster):
    """Verify that NuRaft logged the streaming mode parameters on startup."""
    log = node1.grep_in_log("streaming mode max log gap")
    assert log, "NuRaft startup message with streaming mode parameters not found in log"
    assert "16" in log, f"Expected max_log_gap=16 in log message: {log}"
