import os
import re
import time
import pytest
import random

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def apply_config(config_name):
    """Copy a config file into the container and reload."""
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, f"configs/{config_name}"),
        f"/etc/clickhouse-server/config.d/{config_name}",
    )
    node.query("SYSTEM RELOAD CONFIG")


def remove_config(config_name):
    node.exec_in_container(
        ["rm", "-f", f"/etc/clickhouse-server/config.d/{config_name}"]
    )


def assert_settings(settings_dict):
    """Check that server_settings match expected values and are marked changeable without restart."""
    names = ", ".join(f"'{name}'" for name in settings_dict)
    result = node.query(
        f"SELECT name, value, changeable_without_restart FROM system.server_settings "
        f"WHERE name IN ({names}) ORDER BY name"
    )
    for name, value in settings_dict.items():
        assert f"{name}\t{value}\tYes" in result, (
            f"Expected {name}={value} with changeable_without_restart=Yes, got: {result}"
        )


def reload_with_invalid_config_and_check_log(config_name, group_name):
    """Copy an invalid config and reload, then verify rejection is logged."""
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, f"configs/{config_name}"),
        f"/etc/clickhouse-server/config.d/{config_name}",
    )
    query_id = f"test_invalid_{group_name}_{random.randint(10000, 99999)}"
    node.query("SYSTEM RELOAD CONFIG", query_id=query_id)
    node.query(
        f"SYSTEM FLUSH LOGS text_log; "
        f"SELECT throwIf(count() = 0) FROM system.text_log "
        f"WHERE query_id = '{query_id}' AND message LIKE '%ignore buffer settings for {group_name}%'"
    )


def test_http_buffer_settings(started_cluster):
    """Verify HTTP group socket buffer settings: reload, apply, and reject invalid values."""
    # Defaults are 0
    assert_settings({"http_connections_rcvbuf": 0, "http_connections_sndbuf": 0})

    apply_config("valid_rcvbuf.xml")

    assert_settings({"http_connections_rcvbuf": 262144, "http_connections_sndbuf": 262144})

    # Invalid value should be rejected on reload
    remove_config("valid_rcvbuf.xml")
    reload_with_invalid_config_and_check_log("invalid_rcvbuf.xml", "HTTP")

    remove_config("invalid_rcvbuf.xml")
    node.query("SYSTEM RELOAD CONFIG")


def test_disk_buffer_settings(started_cluster):
    """Verify Disk group socket buffer settings: reload, apply, and reject invalid values."""
    assert_settings({"disk_connections_rcvbuf": 0, "disk_connections_sndbuf": 0})

    apply_config("valid_disk_rcvbuf.xml")

    assert_settings({"disk_connections_rcvbuf": 262144, "disk_connections_sndbuf": 262144})

    remove_config("valid_disk_rcvbuf.xml")
    reload_with_invalid_config_and_check_log("invalid_disk_rcvbuf.xml", "Disk")

    remove_config("invalid_disk_rcvbuf.xml")
    node.query("SYSTEM RELOAD CONFIG")


def test_storage_buffer_settings(started_cluster):
    """Verify Storage group socket buffer settings: reload, apply, and reject invalid values."""
    assert_settings({"storage_connections_rcvbuf": 0, "storage_connections_sndbuf": 0})

    apply_config("valid_storage_rcvbuf.xml")

    assert_settings({"storage_connections_rcvbuf": 262144, "storage_connections_sndbuf": 262144})

    remove_config("valid_storage_rcvbuf.xml")
    reload_with_invalid_config_and_check_log("invalid_storage_rcvbuf.xml", "Storage")

    remove_config("invalid_storage_rcvbuf.xml")
    node.query("SYSTEM RELOAD CONFIG")


def test_receive_buffer_size_applied(started_cluster):
    """Verify that changing storage_connections_rcvbuf is picked up and connections work.

    The url() table function uses the Storage connection group, so we set
    storage_connections_rcvbuf and verify the setting is applied to real sockets.
    """
    buf_size = 65536

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/small_rcvbuf.xml"),
        "/etc/clickhouse-server/config.d/small_rcvbuf.xml",
    )

    node.query("SYSTEM RELOAD CONFIG")

    # Verify the setting is reflected in server_settings.
    result = node.query(
        "SELECT value FROM system.server_settings "
        "WHERE name = 'storage_connections_rcvbuf'"
    )
    assert result.strip() == str(buf_size)

    # Make a slow outgoing HTTP request via the Storage pool to keep the connection alive.
    # url() table function uses the Storage connection group.
    request = node.get_query_request(
        "SELECT * FROM url('http://localhost:8123/?query=SELECT+sleepEachRow(0.1)+FROM+numbers(30)', TSV) FORMAT Null",
    )
    time.sleep(2)

    # Use `ss -tmpn` to inspect sockets and their owning PIDs.
    # Filter to established connections to localhost:8123 owned by the
    # clickhouse-server process (pid obtained from the container).
    pid = node.get_process_pid("clickhouse")
    assert pid is not None, "clickhouse-server process not found"
    # ss -m prints skmem info on the line following the connection line,
    # so use grep -A1 to capture both the connection and its skmem data.
    ss_output = node.exec_in_container(
        [
            "bash",
            "-c",
            f"ss -tmpn state established '( dport = 8123 and dst 127.0.0.1 )' | grep -A1 'pid={pid},' || true",
        ],
        privileged=True,
        user="root",
    )

    # Parse skmem rb field — the kernel receive buffer limit.
    # The kernel doubles the SO_RCVBUF value passed to setsockopt.
    rb_values = re.findall(r"rb(\d+)", ss_output)
    assert len(rb_values) > 0, f"No connections found. ss output: {ss_output}"

    expected_rb = buf_size * 2  # kernel doubles setsockopt value
    matched = any(int(rb) == expected_rb for rb in rb_values)
    assert matched, (
        f"No socket with receive buffer {expected_rb} (2 * {buf_size}) found. "
        f"Actual rb values: {rb_values}. ss output: {ss_output}"
    )

    request.get_answer()

    remove_config("small_rcvbuf.xml")
    node.query("SYSTEM RELOAD CONFIG")
