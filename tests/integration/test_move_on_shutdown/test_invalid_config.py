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
        cluster.start(expected_to_fail=True)
        # If we reach here, the server started when it shouldn't have
        pytest.fail("Server should have failed to start with self-referencing move_on_shutdown_to")
    except Exception as e:
        # Expected: server failed to start
        assert "move_on_shutdown_to pointing to itself" in str(e) or "cannot" in str(e).lower()
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
        cluster.start(expected_to_fail=True)
        pytest.fail("Server should have failed to start with circular move_on_shutdown_to references")
    except Exception as e:
        assert "circular" in str(e).lower() or "cannot" in str(e).lower()
    finally:
        cluster.shutdown()
