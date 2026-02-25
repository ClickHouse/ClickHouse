import pytest

from helpers.cluster import ClickHouseCluster


def test_self_reference_config():
    """Test that self-referencing move_on_shutdown_to fails to start."""
    cluster = ClickHouseCluster(__file__)

    node = cluster.add_instance(
        "node_self_ref",
        main_configs=["configs/config.d/invalid_self_reference.xml"],
        tmpfs=["/test_disk:size=100M"],
        stay_alive=True,
    )

    try:
        cluster.start()
        node.start_clickhouse(start_wait_sec=60, expected_to_fail=True)
        assert node.contains_in_log(
            "move_on_shutdown_to pointing to itself"
        ) or node.contains_in_log("BAD_ARGUMENTS")
    finally:
        cluster.shutdown()


def test_circular_reference_config():
    """Test that circular move_on_shutdown_to references fail to start."""
    cluster = ClickHouseCluster(__file__)

    node = cluster.add_instance(
        "node_circular",
        main_configs=["configs/config.d/invalid_circular.xml"],
        tmpfs=["/test_disk1:size=100M", "/test_disk2:size=100M"],
        stay_alive=True,
    )

    try:
        cluster.start()
        node.start_clickhouse(start_wait_sec=60, expected_to_fail=True)
        assert node.contains_in_log("Circular reference") or node.contains_in_log(
            "BAD_ARGUMENTS"
        )
    finally:
        cluster.shutdown()
