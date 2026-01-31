import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_with_marker = cluster.add_instance(
    "node_with_marker",
    main_configs=["configs/enable_log_marker.xml"],
    stay_alive=True,
)

node_without_marker = cluster.add_instance(
    "node_without_marker",
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def exec_and_flush(node, query):
    node.query(query)
    node.query("SYSTEM FLUSH LOGS")


def get_marker(node, query_pattern):
    return node.query(
        f"""
        SELECT log_marker FROM system.query_log
        WHERE type = 'QueryFinish' AND query LIKE '{query_pattern}'
        ORDER BY event_time DESC LIMIT 1
        """
    ).strip()


def test_log_marker_enabled(start_cluster):
    """Test log_marker column with valid UUIDs when enabled"""
    exec_and_flush(node_with_marker, "SELECT 1")

    result = node_with_marker.query(
        "SELECT type FROM system.columns "
        "WHERE database='system' AND table='query_log' AND name='log_marker'"
    ).strip()
    assert result == "UUID"

    # Verify non-NULL, non-zero UUIDs
    result = node_with_marker.query(
        """
        SELECT countIf(log_marker != toUUID('00000000-0000-0000-0000-000000000000')) = count()
        FROM system.query_log
        WHERE type = 'QueryFinish'
        """
    ).strip()
    assert result == "1"


def test_log_marker_with_different_batches(start_cluster):
    """Test that same flush batch shares marker, different batches have different markers"""
    markers = []

    # Test with separate flush batches
    for i in range(3):
        exec_and_flush(node_with_marker, f"SELECT {i}")
        markers.append(get_marker(node_with_marker, f"SELECT {i}"))

    # All markers should be valid and unique across batches
    assert len(markers) == 3
    assert (
        len(set(markers)) == 3
    ), "Different flush batches should have different markers"
    assert all(markers), "All markers should be non-empty"


def test_log_marker_disabled(start_cluster):
    """Test that log_marker column exists but is not populated when feature is disabled"""
    exec_and_flush(node_without_marker, "SELECT 1")

    # Column should exist in schema but all the values should be zero UUID (not populated)
    result = node_without_marker.query(
        "SELECT type FROM system.columns "
        "WHERE database='system' AND table='query_log' AND name='log_marker'"
    ).strip()
    assert result == "UUID", "log_marker column should exist in schema"

    result = node_without_marker.query(
        """
        SELECT countIf(log_marker = toUUID('00000000-0000-0000-0000-000000000000')) = count()
        FROM system.query_log
        WHERE type = 'QueryFinish'
        """
    ).strip()
    assert (
        result == "1"
    ), "All log_marker values should be zero UUID when feature is disabled"
