import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/dedup_version.xml"],
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/dedup_version.xml"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_supported_version_starts(start_cluster):
    # The only supported value, new_unified_hash, starts normally.
    assert node.get_process_pid("clickhouse") is not None
    assert node.query("SELECT 1") == "1\n"


def test_unsupported_version_aborts_startup(start_cluster):
    # This build writes only the unified deduplication hash, so any other insert_deduplication_version
    # must fail-close at startup with a migration message rather than silently degrading deduplication.
    node.stop_clickhouse()
    node.replace_in_config(CONFIG_PATH, "new_unified_hash", "compatible_double_hashes")
    node.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

    assert node.get_process_pid("clickhouse") is None
    assert node.contains_in_log(
        "supports only the unified insert deduplication hash"
    )

    # Restore the supported value so the server (and module teardown) is healthy again.
    node.replace_in_config(CONFIG_PATH, "compatible_double_hashes", "new_unified_hash")
    node.start_clickhouse()
    assert node.get_process_pid("clickhouse") is not None
