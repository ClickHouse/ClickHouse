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

UPDATED_DYNAMIC_CONFIG = """
<clickhouse>
    <keeper_server>
        <coordination_settings>
            <snapshot_distance>99999</snapshot_distance>
            <max_requests_batch_size>42</max_requests_batch_size>
            <quorum_reads>true</quorum_reads>
            <max_request_size>1024</max_request_size>
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

    # 2. Reload config with max_request_size=1024.
    with node.with_replace_config(DYNAMIC_CONFIG_PATH, UPDATED_DYNAMIC_CONFIG, reload_after=True):
        for _ in range(30):
            time.sleep(1)
            settings = get_coordination_settings(node)
            if settings.get("max_request_size") == "1024":
                break
        else:
            assert False, (
                "max_request_size did not change to 1024 after config reload; "
                f"current value: {settings.get('max_request_size')}"
            )

        # 3. A large write should now be rejected.
        with pytest.raises(Exception, match=r"Connection loss"):
            node.query(
                "INSERT INTO system.zookeeper (name, path, value) "
                "SELECT number::String, '/test_max_req', repeat('x', 3000) "
                "FROM numbers(100)"
            )
