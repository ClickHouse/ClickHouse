import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/memory.xml"], stay_alive=True)

CONFIG_PATH = "/etc/clickhouse-server/config.d/memory.xml"

STARTUP_VALUE = 8 * 1024 * 1024
UINT64_MAX = 18446744073709551615
INT64_MAX = 9223372036854775807


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def live_value():
    return int(
        node.query(
            "SELECT value FROM system.server_settings "
            "WHERE name = 'additional_memory_tracking_per_thread'",
            settings={"max_threads": 1},
        ).strip()
    )


def changeable_without_restart():
    return node.query(
        "SELECT changeable_without_restart FROM system.server_settings "
        "WHERE name = 'additional_memory_tracking_per_thread'",
        settings={"max_threads": 1},
    ).strip()


def reload_with(value):
    node.replace_in_config(
        CONFIG_PATH,
        "<additional_memory_tracking_per_thread>[0-9]*</additional_memory_tracking_per_thread>",
        f"<additional_memory_tracking_per_thread>{value}</additional_memory_tracking_per_thread>",
    )
    node.query("SYSTEM RELOAD CONFIG")


def test_oversized_value_is_clamped_on_reload(start_cluster):
    """The oversized value must be clamped on the real `SYSTEM RELOAD CONFIG` path of the server.

    `additional_memory_tracking_per_thread` is a `UInt64` setting, but the value is added as a
    signed delta into the total `MemoryTracker`, so an unclamped near-`UInt64` maximum would wrap
    the tracker negative. The server clamps it to the memory of the machine; this test asserts the
    published live value, which is the only observable of that clamp.
    """

    assert changeable_without_restart() == "Yes"
    assert live_value() == STARTUP_VALUE

    try:
        # A sane reload must publish the new value, not the startup one.
        reload_with(16 * 1024 * 1024)
        assert live_value() == 16 * 1024 * 1024

        # The oversized reload must publish neither the raw value nor the previous one.
        reload_with(UINT64_MAX)
        clamped = live_value()
        assert clamped != UINT64_MAX
        assert clamped != 16 * 1024 * 1024
        assert 1 <= clamped <= INT64_MAX

        # The clamp is the memory of the machine. The exact amount depends on the cgroup limits of
        # the container, so compare against what the server itself reports as its physical memory.
        os_memory_total = int(
            node.query(
                "SELECT toUInt64(value) FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal'",
                settings={"max_threads": 1},
            ).strip()
        )
        assert clamped <= os_memory_total

        # Removing the override restores the startup value.
        reload_with(STARTUP_VALUE)
        assert live_value() == STARTUP_VALUE
    finally:
        reload_with(STARTUP_VALUE)
