import os
import re
import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/prom_conf.xml"])
node_labels = cluster.add_instance(
    "node_labels",
    main_configs=["configs/prom_conf_labels.xml"],
    # The `shard` label is defined with from_env in the config; this exercises env-based resolution.
    # `instance_env_variables=True` is required here: by default `env_variables` is merged into the
    # whole cluster's shared env file, not scoped to this instance, so without it `node_handler_labels`'s
    # own `PROM_SHARD` below would silently overwrite this one for every instance in the cluster.
    env_variables={"PROM_SHARD": "shard-01"},
    instance_env_variables=True,
)
node_group_label_disabled = cluster.add_instance(
    "node_group_label_disabled",
    main_configs=["configs/prom_conf_group_label_disabled_sections.xml"],
)
node_handler_labels = cluster.add_instance(
    "node_handler_labels",
    main_configs=["configs/prom_conf_handlers_labels.xml"],
    # Exercises the from_env path for a handler defined inside a `prometheus.handlers` block.
    env_variables={"PROM_SHARD": "shard-02"},
    instance_env_variables=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def parse_response_line(line):
    allowed_prefixes = [
        "ClickHouse",
        "# HELP",
        "# TYPE",
    ]
    assert any(line.startswith(prefix) for prefix in allowed_prefixes)

    if line.startswith("#"):
        return {}
    match = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]+)(\{.*\})? -?(\d+)", line)
    assert match, line
    name, _, val = match.groups()
    return {name: int(val)}


def get_metrics_response(instance, retries):
    while True:
        try:
            response = requests.get(
                "http://{host}:{port}/metrics".format(
                    host=instance.ip_address, port=8001
                ),
                allow_redirects=False,
                # less than default keep-alive timeout (10 seconds)
                timeout=5,
            )

            if response.status_code != 200:
                response.raise_for_status()

            break
        except:
            if retries >= 0:
                retries -= 1
                time.sleep(0.5)
                continue
            else:
                raise

    assert response.headers["content-type"].startswith("text/plain")
    return response


def get_and_check_metrics(retries):
    response = get_metrics_response(node, retries)

    results = {}
    for resp_line in response.text.split("\n"):
        resp_line = resp_line.rstrip()
        if not resp_line:
            continue
        res = parse_response_line(resp_line)
        results.update(res)
    return results


def test_prometheus_endpoint(start_cluster):
    metrics_dict = get_and_check_metrics(10)
    assert metrics_dict["ClickHouseProfileEvents_Query"] >= 0
    prev_query_count = metrics_dict["ClickHouseProfileEvents_Query"]

    node.query("SELECT 1")
    node.query("SELECT 2")
    node.query("SELECT 3")

    metrics_dict = get_and_check_metrics(10)
    assert metrics_dict["ClickHouseProfileEvents_Query"] >= prev_query_count + 3

    node.query_and_get_error(
        "SELECT throwIf(1, 'test', toInt16(42)) SETTINGS allow_custom_error_code_in_throwif=1"
    )
    metrics_dict = get_and_check_metrics(10)

    assert metrics_dict["ClickHouseErrorMetric_NUMBER_OF_ARGUMENTS_DOESNT_MATCH"] >= 1
    assert metrics_dict["ClickHouseErrorMetric_ALL"] >= 1
    assert metrics_dict["ClickHouse_Info"] == 1


def test_prometheus_endpoint_constant_labels(start_cluster):
    node_labels.query("SELECT 1")

    response = get_metrics_response(node_labels, 10)

    for line in response.text.split("\n"):
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        # Every exposed metric must carry the constant labels from the config. `shard` is defined with
        # from_env="PROM_SHARD" (set to "shard-01"), so asserting its value also verifies that env-based
        # label resolution works end-to-end (config preprocessing -> label enumeration -> /metrics).
        assert 'environment="staging"' in line, line
        assert 'shard="shard-01"' in line, line
        # The unresolved placeholder must never leak into the output.
        assert "PROM_SHARD" not in line, line

    # Constant labels are merged with the metric's own labels.
    assert (
        'ClickHouse_Info{environment="staging",shard="shard-01",name="'
        in response.text
    )
    assert re.search(
        r'^ClickHouseProfileEvents_Query\{environment="staging",shard="shard-01"\} \d+',
        response.text,
        re.MULTILINE,
    )


def test_prometheus_endpoint_constant_labels_in_handlers_config(start_cluster):
    # Constant labels configured inside a `<prometheus><handlers>` block go through the
    # "prometheus.handlers.*.handler.labels" config prefix, which is a different handler-construction
    # path than the top-level "prometheus.labels" section exercised above.
    node_handler_labels.query("SELECT 1")

    response = get_metrics_response(node_handler_labels, 10)

    for line in response.text.split("\n"):
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        assert 'environment="staging"' in line, line
        assert 'shard="shard-02"' in line, line
        assert "PROM_SHARD" not in line, line

    assert (
        'ClickHouse_Info{environment="staging",shard="shard-02",name="'
        in response.text
    )
    assert re.search(
        r'^ClickHouseProfileEvents_Query\{environment="staging",shard="shard-02"\} \d+',
        response.text,
        re.MULTILINE,
    )


def test_prometheus_endpoint_constant_label_allowed_when_section_disabled(start_cluster):
    # `group` is a histogram-family label, but this endpoint has histograms and dimensional metrics
    # disabled, so no exported sample can contain a `group` label. The reserved-name check must be
    # derived from the actual export surface, so `group` is accepted here (server starts and serves).
    node_group_label_disabled.query("SELECT 1")

    response = get_metrics_response(node_group_label_disabled, 10)

    saw_metric = False
    for line in response.text.split("\n"):
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        # The constant label is present on every sample and never duplicated.
        assert 'group="prod"' in line, line
        assert line.count("group=") == 1, line
        saw_metric = True
    assert saw_metric


def test_prometheus_endpoint_reserved_label():
    # A distinct name keeps this cluster's docker compose project and instances directory separate
    # from the module-scoped cluster; otherwise starting/stopping it destroys the shared cluster.
    reserved_label_cluster = ClickHouseCluster(__file__, name="reserved_label")
    reserved_label_cluster.add_instance(
        "node_reserved_label",
        main_configs=["configs/prom_conf_reserved_label.xml"],
    )

    try:
        # The "le" label is reserved because it always is written for histogram buckets,
        # so it cannot be also configured as a constant label.
        with pytest.raises(Exception):
            reserved_label_cluster.start()

        logs = ""
        error_logs_file = os.path.join(
            reserved_label_cluster.instances_dir,
            "node_reserved_label",
            "logs",
            "clickhouse-server.err.log",
        )
        with open(error_logs_file, "r") as f:
            logs = f.read()

        assert (
            "Invalid Prometheus label name 'le' in the configuration: this name is reserved"
            in logs
        )
    finally:
        reserved_label_cluster.shutdown()


def test_prometheus_endpoint_reserved_family_label():
    # A distinct name keeps this cluster's docker compose project and instances directory separate
    # from the module-scoped cluster; otherwise starting/stopping it destroys the shared cluster.
    reserved_family_label_cluster = ClickHouseCluster(__file__, name="reserved_family_label")
    reserved_family_label_cluster.add_instance(
        "node_reserved_family_label",
        main_configs=["configs/prom_conf_reserved_family_label.xml"],
    )

    try:
        # "group" is a per-sample label of histogram/dimensional metric families, so it cannot be
        # also configured as a constant label - otherwise a sample would carry two "group" labels.
        with pytest.raises(Exception):
            reserved_family_label_cluster.start()

        logs = ""
        error_logs_file = os.path.join(
            reserved_family_label_cluster.instances_dir,
            "node_reserved_family_label",
            "logs",
            "clickhouse-server.err.log",
        )
        with open(error_logs_file, "r") as f:
            logs = f.read()

        assert (
            "Invalid Prometheus label name 'group' in the configuration: this name is reserved"
            in logs
        )
    finally:
        reserved_family_label_cluster.shutdown()
