import time

import pytest
import requests

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The `DiskTotal` family is used everywhere below: the `default` disk always exists, and the metric is a
# gauge, so it is present right after the first update of the asynchronous metrics.
KEY_VALUES_NAME = "DiskTotal"
LEGACY_NAME = "DiskTotal_default"

MODE_CONFIG_PATH = "/etc/clickhouse-server/config.d/key_values.xml"

node_key_values = cluster.add_instance(
    "node_key_values",
    main_configs=["configs/prometheus.xml", "configs/asynchronous_metric_log.xml"],
)
node_legacy_names = cluster.add_instance(
    "node_legacy_names",
    main_configs=[
        "configs/prometheus.xml",
        "configs/asynchronous_metric_log.xml",
        "configs/legacy_names.xml",
    ],
)
node_both = cluster.add_instance(
    "node_both",
    main_configs=[
        "configs/prometheus.xml",
        "configs/asynchronous_metric_log.xml",
        "configs/both.xml",
    ],
)
node_reload = cluster.add_instance(
    "node_reload",
    main_configs=[
        "configs/prometheus.xml",
        "configs/asynchronous_metric_log.xml",
        "configs/key_values.xml",
    ],
)
# A Prometheus endpoint with a constant `disk` label, which is the label the key-value form writes the
# disk name into. Such a configuration is only valid while that form is not published.
node_legacy_names_labels = cluster.add_instance(
    "node_legacy_names_labels",
    main_configs=[
        "configs/prometheus_with_labels.xml",
        "configs/asynchronous_metric_log.xml",
        "configs/legacy_names.xml",
    ],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_current_metrics(node):
    """Whether `system.asynchronous_metrics` holds the key-value form and/or the legacy scalar one."""
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    key_values = node.query(
        f"SELECT key_values['default'] > 0 FROM system.asynchronous_metrics WHERE metric = '{KEY_VALUES_NAME}'"
    ).strip()
    legacy = node.query(
        f"SELECT value > 0 FROM system.asynchronous_metrics WHERE metric = '{LEGACY_NAME}'"
    ).strip()
    return key_values, legacy


def get_logged_metrics(node):
    """The number of rows of both forms in `system.asynchronous_metric_log`."""
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    node.query("SYSTEM FLUSH LOGS asynchronous_metric_log")
    key_values = node.query(
        f"SELECT count() FROM system.asynchronous_metric_log WHERE metric = '{KEY_VALUES_NAME}' AND key = 'default'"
    ).strip()
    legacy = node.query(
        f"SELECT count() FROM system.asynchronous_metric_log WHERE metric = '{LEGACY_NAME}' AND key = ''"
    ).strip()
    return int(key_values), int(legacy)


def fetch_prometheus_metrics(node, retries=10):
    """The raw Prometheus exposition of the node."""
    while True:
        try:
            response = requests.get(
                f"http://{node.ip_address}:8001/metrics",
                allow_redirects=False,
                timeout=5,
            )
            if response.status_code != 200:
                response.raise_for_status()
            return response.text
        except:
            if retries <= 0:
                raise
            retries -= 1
            time.sleep(0.5)


def get_prometheus_metrics(node):
    """Whether the Prometheus endpoint exposes the labelled sample and/or the legacy mangled name."""
    key_values = False
    legacy = False
    for line in fetch_prometheus_metrics(node).split("\n"):
        if line.startswith(
            f'ClickHouseAsyncMetrics_{KEY_VALUES_NAME}{{disk="default"}} '
        ):
            key_values = True
        if line.startswith(f"ClickHouseAsyncMetrics_{LEGACY_NAME} "):
            legacy = True
    return key_values, legacy


def test_key_values_mode(start_cluster):
    """The default: only the key-value form of a key-value metric is published."""
    assert get_current_metrics(node_key_values) == ("1", "")
    assert get_logged_metrics(node_key_values)[1] == 0
    assert get_logged_metrics(node_key_values)[0] > 0
    assert get_prometheus_metrics(node_key_values) == (True, False)


def test_legacy_names_mode(start_cluster):
    """Only the pre-26.8 form: one scalar metric per key, with the key mangled into the name."""
    assert get_current_metrics(node_legacy_names) == ("", "1")
    assert get_logged_metrics(node_legacy_names)[0] == 0
    assert get_logged_metrics(node_legacy_names)[1] > 0
    assert get_prometheus_metrics(node_legacy_names) == (False, True)


def test_both_modes(start_cluster):
    """Both forms at the same time, so that the monitoring can be migrated without a gap."""
    assert get_current_metrics(node_both) == ("1", "1")
    logged_key_values, logged_legacy = get_logged_metrics(node_both)
    assert logged_key_values > 0
    assert logged_legacy > 0
    assert get_prometheus_metrics(node_both) == (True, True)


def test_switching_the_mode_without_a_restart(start_cluster):
    """The mode is re-read on every update of the asynchronous metrics, so `SYSTEM RELOAD CONFIG` is enough."""
    assert get_current_metrics(node_reload) == ("1", "")

    node_reload.replace_in_config(MODE_CONFIG_PATH, ">key_values<", ">legacy_names<")
    node_reload.query("SYSTEM RELOAD CONFIG")

    assert get_current_metrics(node_reload) == ("", "1")
    assert get_prometheus_metrics(node_reload) == (False, True)
    assert (
        node_reload.query(
            "SELECT value, changeable_without_restart FROM system.server_settings"
            " WHERE name = 'asynchronous_metrics_key_values_mode'"
        ).strip()
        == "legacy_names\tYes"
    )

    node_reload.replace_in_config(MODE_CONFIG_PATH, ">legacy_names<", ">both<")
    node_reload.query("SYSTEM RELOAD CONFIG")

    assert get_current_metrics(node_reload) == ("1", "1")
    assert get_prometheus_metrics(node_reload) == (True, True)


def test_legacy_names_mode_frees_the_key_labels(start_cluster):
    """A constant label may reuse an asynchronous metric key label while the key-value form is not published."""
    node_legacy_names_labels.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    lines = fetch_prometheus_metrics(node_legacy_names_labels).split("\n")
    assert any(
        line.startswith(
            f'ClickHouseAsyncMetrics_{LEGACY_NAME}{{disk="the_whole_node"}} '
        )
        for line in lines
    )


def test_switching_back_rejects_the_reload_and_changes_nothing(start_cluster):
    """Switching the same node into the key-value form would make the constant label collide with the key
    label, so the whole reload is rejected before it takes effect, instead of exposing two `disk` labels.
    """
    node_legacy_names_labels.replace_in_config(
        "/etc/clickhouse-server/config.d/legacy_names.xml",
        ">legacy_names<",
        ">key_values<",
    )

    with pytest.raises(
        QueryRuntimeException, match="Invalid Prometheus label name 'disk'"
    ):
        node_legacy_names_labels.query("SYSTEM RELOAD CONFIG")

    # The rejected configuration is not installed: the published form, the exposed metrics and the
    # reported setting value all stay as they were, and the endpoint keeps serving.
    assert get_current_metrics(node_legacy_names_labels) == ("", "1")
    assert (
        node_legacy_names_labels.query(
            "SELECT value FROM system.server_settings"
            " WHERE name = 'asynchronous_metrics_key_values_mode'"
        ).strip()
        == "legacy_names"
    )

    node_legacy_names_labels.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    lines = fetch_prometheus_metrics(node_legacy_names_labels).split("\n")
    assert any(
        line.startswith(
            f'ClickHouseAsyncMetrics_{LEGACY_NAME}{{disk="the_whole_node"}} '
        )
        for line in lines
    )

    # Removing the constant label makes the same switch acceptable again.
    node_legacy_names_labels.replace_in_config(
        "/etc/clickhouse-server/config.d/prometheus_with_labels.xml",
        "<disk>the_whole_node</disk>",
        "",
    )
    node_legacy_names_labels.query("SYSTEM RELOAD CONFIG")

    assert get_current_metrics(node_legacy_names_labels) == ("1", "")
    assert get_prometheus_metrics(node_legacy_names_labels) == (True, False)
