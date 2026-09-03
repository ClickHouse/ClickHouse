import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")

# A local disk and a cache disk: both probe their root from their constructor, and the reload
# publishes what the probes recorded only after every lock it took has been released.
DISKS_CONFIG = """
<clickhouse>
    <storage_configuration>
        <disks>
            <local_{suffix}>
                <type>local</type>
                <path>/var/lib/clickhouse/disks/local_{suffix}/</path>
            </local_{suffix}>
            <blob_{suffix}>
                <type>local_blob_storage</type>
                <path>/var/lib/clickhouse/disks/blob_{suffix}/</path>
            </blob_{suffix}>
            <cache_{suffix}>
                <type>cache</type>
                <disk>blob_{suffix}</disk>
                <path>cache_{suffix}/</path>
                <max_size>1Mi</max_size>
                <skip_access_check>true</skip_access_check>
            </cache_{suffix}>
        </disks>
    </storage_configuration>
</clickhouse>
"""

ADDED_DISKS_QUERY = "SELECT count() FROM system.disks WHERE name IN ('local_{suffix}', 'blob_{suffix}', 'cache_{suffix}')"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        # The disk selector is built lazily and a reload only updates one that already exists.
        node.query("SELECT count() FROM system.disks")
        yield cluster
    finally:
        cluster.shutdown()


def add_disks(suffix):
    with open(os.path.join(node.config_d_dir, f"disks_{suffix}.xml"), "w") as f:
        f.write(DISKS_CONFIG.format(suffix=suffix))


def test_system_reload_config_adds_disks():
    add_disks("reloaded")
    # A publish left under one of the reload's locks deadlocks here instead of returning.
    node.query("SYSTEM RELOAD CONFIG", timeout=60)

    assert node.query(ADDED_DISKS_QUERY.format(suffix="reloaded"), timeout=60) == "3\n"
    warnings = node.query("SELECT count() FROM system.warnings", timeout=60)
    assert warnings.strip().isdigit()


def test_background_config_reloader_adds_disks():
    add_disks("polled")

    assert_eq_with_retry(
        node,
        ADDED_DISKS_QUERY.format(suffix="polled"),
        "3",
        retry_count=30,
        sleep_time=1,
        timeout=60,
    )
    warnings = node.query("SELECT count() FROM system.warnings", timeout=60)
    assert warnings.strip().isdigit()
