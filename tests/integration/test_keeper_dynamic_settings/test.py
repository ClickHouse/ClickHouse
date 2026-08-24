import csv
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/keeper_config.xml",
        "configs/keeper_dynamic.xml",
    ],
    stay_alive=True,
)
node_ttl = cluster.add_instance(
    "node_ttl",
    main_configs=[
        "configs/keeper_config_ttl.xml",
        "configs/keeper_dynamic_ttl.xml",
    ],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_coordination_settings(node):
    """Query the 'conf' 4-letter command and return settings as a dict."""
    data = keeper_utils.send_4lw_cmd(cluster, node, cmd="conf")
    reader = csv.reader(data.split("\n"), delimiter="=")
    result = {}
    for row in reader:
        if len(row) >= 2:
            result[row[0]] = row[1]
    return result


DYNAMIC_CONFIG_PATH = "/etc/clickhouse-server/config.d/keeper_dynamic.xml"

UPDATED_MAX_REQUEST_SIZE = "10240"
UPDATED_DYNAMIC_CONFIG = f"""
<clickhouse>
    <keeper_server>
        <coordination_settings>
            <snapshot_distance>99999</snapshot_distance>
            <max_requests_batch_size>42</max_requests_batch_size>
            <quorum_reads>true</quorum_reads>
            <max_request_size>{UPDATED_MAX_REQUEST_SIZE}</max_request_size>
        </coordination_settings>
    </keeper_server>
</clickhouse>
"""


def test_dynamic_settings_hot_reload(started_cluster):
    """Verify that settings marked as HOT_RELOAD are updated after config
    reload, while non-reloadable settings remain unchanged."""

    keeper_utils.wait_until_connected(cluster, node)

    # 1. Check initial values via the 'conf' 4-letter command.
    settings = get_coordination_settings(node)
    assert settings["max_requests_batch_size"] == "100"
    assert settings["quorum_reads"] == "false"
    assert settings["snapshot_distance"] == "75"

    # 2. Replace the config file in-place.  ClickHouse picks up config
    #    changes automatically (ConfigReloader), so no restart is needed.
    with node.with_replace_config(DYNAMIC_CONFIG_PATH, UPDATED_DYNAMIC_CONFIG, reload_after=True):
        # 3. Wait for the config reload to take effect.
        for _ in range(30):
            time.sleep(1)
            settings = get_coordination_settings(node)
            if settings.get("max_requests_batch_size") == "42":
                break
        else:
            assert False, (
                "max_requests_batch_size did not change to 42 after config reload; "
                f"current value: {settings.get('max_requests_batch_size')}"
            )

        # 4. Verify the other HOT_RELOAD setting changed too.
        assert settings["quorum_reads"] == "true"

        # 5. Verify that a non-HOT_RELOAD setting was NOT updated.
        assert settings["snapshot_distance"] == "75", (
            "snapshot_distance should not change via hot reload, "
            f"but got {settings['snapshot_distance']}"
        )


def test_max_request_size_hot_reload(started_cluster):
    """Start with max_request_size=0 (unlimited), write a large node,
    hot-reload to a small limit, verify that large writes are rejected."""

    keeper_utils.wait_until_connected(cluster, node)

    # 1. Initially max_request_size is 0 (unlimited) — large writes work.
    settings = get_coordination_settings(node)
    assert settings["max_request_size"] == "0"

    node.query(
        "INSERT INTO system.zookeeper (name, path, value) "
        "VALUES ('big_before', '/test_max_req', repeat('x', 3000))"
    )

    # 2. Reload config with a small max_request_size
    with node.with_replace_config(DYNAMIC_CONFIG_PATH, UPDATED_DYNAMIC_CONFIG, reload_after=True):
        for _ in range(30):
            time.sleep(1)
            settings = get_coordination_settings(node)
            if settings.get("max_request_size") == UPDATED_MAX_REQUEST_SIZE:
                break
        else:
            assert False, (
                "max_request_size hasn't been updated after config reload; "
                f"current value: {settings.get('max_request_size')}"
            )

        # 3. A large write should now be rejected. `Connection loss`, `Operation timeout`
        # and `exceeds limit` are valid client surfaces of the connection-level rejection.
        with pytest.raises(Exception, match=r"exceeds limit|Connection loss|Operation timeout"):
            node.query(
                "INSERT INTO system.zookeeper (name, path, value) "
                "SELECT number::String, '/test_max_req', repeat('x', 3000) "
                "FROM numbers(100)"
            )

        # 4. A fresh session learns the advertised limit at connect, so after a
        # restart the rejection is client-side and deterministic (`exceeds limit`).
        node.restart_clickhouse()
        keeper_utils.wait_until_connected(cluster, node)
        with pytest.raises(Exception, match="exceeds limit"):
            node.query(
                "INSERT INTO system.zookeeper (name, path, value) "
                "SELECT number::String, '/test_max_req', repeat('x', 3000) "
                "FROM numbers(100)"
            )


SNAPSHOT_DIR = "/var/lib/clickhouse/coordination/snapshots"

UPDATED_SNAPSHOT_VERSION_CONFIG = """
<clickhouse>
    <keeper_server>
        <coordination_settings>
            <snapshot_distance>75</snapshot_distance>
            <max_requests_batch_size>100</max_requests_batch_size>
            <quorum_reads>false</quorum_reads>
            <write_snapshot_version>9</write_snapshot_version>
        </coordination_settings>
    </keeper_server>
</clickhouse>
"""


def create_snapshot_and_get_version(marker):
    """Advance the log, force a snapshot via 'csnp' and return the version
    byte (the first byte of the decompressed snapshot file)."""

    # Write something so the log advances and 'csnp' produces a new snapshot.
    node.query(
        "INSERT INTO system.zookeeper (name, path, value) "
        f"VALUES ('{marker}', '/test_snapshot_version', 'somedata')"
    )

    snapshot_idx = keeper_utils.send_4lw_cmd(cluster, node, cmd="csnp").strip()
    assert snapshot_idx.isdigit(), f"csnp did not return a log index: {snapshot_idx!r}"
    node.wait_for_log_line(f"Created persistent snapshot {snapshot_idx} with path")

    version_byte = node.exec_in_container(
        [
            "bash",
            "-c",
            f'zstd -dc "$(ls -t {SNAPSHOT_DIR}/*.bin.zstd | head -n1)" | od -An -tu1 -N1',
        ]
    )
    return int(version_byte)


def test_write_snapshot_version_hot_reload(started_cluster):
    """Bump write_snapshot_version via config reload (no restart) and verify
    that newly created snapshots are written in the new format version."""

    keeper_utils.wait_until_connected(cluster, node)

    # 1. Initially snapshots are written with version 6.
    settings = get_coordination_settings(node)
    assert settings["write_snapshot_version"] == "6"
    assert create_snapshot_and_get_version("before_reload") == 6

    # 2. Push a new config with write_snapshot_version=9 without restart.
    with node.with_replace_config(
        DYNAMIC_CONFIG_PATH, UPDATED_SNAPSHOT_VERSION_CONFIG, reload_after=True
    ):
        for _ in range(30):
            time.sleep(1)
            settings = get_coordination_settings(node)
            if settings.get("write_snapshot_version") == "9":
                break
        else:
            assert False, (
                "write_snapshot_version did not change to 9 after config reload; "
                f"current value: {settings.get('write_snapshot_version')}"
            )

        # 3. The next snapshot must be written with the new version.
        assert create_snapshot_and_get_version("after_reload") == 9


DYNAMIC_TTL_CONFIG_PATH = "/etc/clickhouse-server/config.d/keeper_dynamic_ttl.xml"

DOWNGRADED_SNAPSHOT_VERSION_CONFIG = """
<clickhouse>
    <keeper_server>
        <coordination_settings>
            <write_snapshot_version>6</write_snapshot_version>
        </coordination_settings>
    </keeper_server>
</clickhouse>
"""


def test_write_snapshot_version_reload_rejected(started_cluster):
    """write_snapshot_version must not be hot-reloadable below what the enabled
    feature flags require: node_ttl has CREATE_TTL enabled, which needs snapshot
    version >= 8, so a reload lowering it to 6 must be rejected and the old
    value kept."""

    keeper_utils.wait_until_connected(cluster, node_ttl)

    settings = get_coordination_settings(node_ttl)
    assert settings["write_snapshot_version"] == "8"

    with node_ttl.with_replace_config(
        DYNAMIC_TTL_CONFIG_PATH, DOWNGRADED_SNAPSHOT_VERSION_CONFIG, reload_after=True
    ):
        # The config reloader picks up the change, the validation throws and the
        # new settings are discarded.
        node_ttl.wait_for_log_line(
            "Feature flag CREATE_TTL requires write_snapshot_version"
        )

        settings = get_coordination_settings(node_ttl)
        assert settings["write_snapshot_version"] == "8", (
            "write_snapshot_version incompatible with CREATE_TTL was accepted "
            f"on reload: {settings['write_snapshot_version']}"
        )
