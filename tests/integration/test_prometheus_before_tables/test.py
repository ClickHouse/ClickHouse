import re
import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/prom_conf.xml"])
# A configuration with custom `prometheus.handlers` must keep the regular server lifecycle.
node_handlers = cluster.add_instance("node_handlers", main_configs=["configs/prom_conf_handlers.xml"])
# A separate metrics-only instance for the destructive startup-failure test.
# Disable `with_remote_database_disk`: the test injects a broken metadata file into the local
# metadata directory, which the server would not read if database metadata lived on a remote disk.
node_fail = cluster.add_instance(
    "node_fail",
    main_configs=["configs/prom_conf.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

LOG_FILE = "/var/log/clickhouse-server/clickhouse-server.log"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def first_log_line_number(pattern, instance=None):
    # `grep -n` prints "<line>:<text>"; `-m1` stops at the first match.
    output = (instance or node).exec_in_container(["bash", "-c", f"grep -n -m1 '{pattern}' {LOG_FILE}"])
    assert output, f"log line not found: {pattern}"
    return int(output.split(":", 1)[0])


def get_metrics(retries=10):
    last_exc = None
    for _ in range(retries + 1):
        try:
            response = requests.get(
                "http://{host}:{port}/metrics".format(host=node.ip_address, port=8001),
                allow_redirects=False,
                # less than default keep-alive timeout (10 seconds)
                timeout=5,
            )
            response.raise_for_status()
            break
        except Exception as exc:
            last_exc = exc
            time.sleep(0.5)
    else:
        raise last_exc

    assert response.headers["content-type"].startswith("text/plain")

    results = {}
    for line in response.text.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]+)(\{.*\})?\s+(-?[\d.eE+]+)", line)
        assert match, line
        name, _, val = match.groups()
        results[name] = float(val)
    return results


def test_prometheus_starts_before_tables_are_loaded(start_cluster):
    # A metrics-only Prometheus endpoint (no `prometheus.handlers`) is started before metadata
    # (tables) loading begins. This keeps metrics observable during the (potentially long) metadata
    # loading phase. Previously it would start only after "Loaded metadata.".
    # Anchor on the `Application:` logger prefix: other components log similar messages, e.g. with
    # the database on a plain disk, `MetadataStorageFromPlainObjectStorage` logs
    # "Loaded metadata (empty)" during disk initialization, long before tables are loaded.
    prometheus_listen_line = first_log_line_number("Application: Listening for Prometheus")
    loaded_metadata_line = first_log_line_number("Application: Loaded metadata")
    assert prometheus_listen_line < loaded_metadata_line


def test_prometheus_exposes_metrics(start_cluster):
    metrics = get_metrics()
    # Profile events and current metrics are available immediately.
    assert metrics["ClickHouseProfileEvents_Query"] >= 0
    # Asynchronous metrics are collected by a thread that is now started before tables are loaded;
    # check that they are still exposed (e.g. the server uptime).
    assert any(name.startswith("ClickHouseAsyncMetrics_") for name in metrics)


def test_system_stop_start_listen(start_cluster):
    # Although the Prometheus server is started early, it lives in the regular `servers` list, so
    # the `SYSTEM START/STOP LISTEN` contract must keep working for it.
    node.query("SYSTEM STOP LISTEN PROMETHEUS")
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.get(
            "http://{host}:{port}/metrics".format(host=node.ip_address, port=8001),
            allow_redirects=False,
            timeout=5,
        )
    node.query("SYSTEM START LISTEN PROMETHEUS")
    metrics = get_metrics()
    assert metrics["ClickHouseProfileEvents_Query"] >= 0


def test_prometheus_with_handlers_starts_after_tables_are_loaded(start_cluster):
    # Custom `prometheus.handlers` may serve queries (`remote_write`, `remote_read`, `query`,
    # `api_v1`), so such configurations must keep the regular server lifecycle: the endpoint must
    # not accept requests before metadata and access control are ready, i.e. the early-start path
    # must be skipped.
    prometheus_listen_line = first_log_line_number("Application: Listening for Prometheus", node_handlers)
    loaded_metadata_line = first_log_line_number("Application: Loaded metadata", node_handlers)
    assert loaded_metadata_line < prometheus_listen_line

    # The handler-based endpoint still works after startup.
    response = requests.get(
        "http://{host}:{port}/metrics".format(host=node_handlers.ip_address, port=8001),
        allow_redirects=False,
        timeout=5,
    )
    response.raise_for_status()
    assert "ClickHouseProfileEvents_Query" in response.text


def test_startup_failure_after_early_prometheus_bind(start_cluster):
    # If startup fails after the early Prometheus bind (e.g. while loading metadata), the cleanup
    # path must stop the already-listening server; otherwise `server_pool.joinAll` would wait for
    # its listener thread forever and the process would hang instead of exiting.
    node_fail.stop_clickhouse()
    node_fail.exec_in_container(
        [
            "bash",
            "-c",
            "echo 'NOT A VALID ATTACH QUERY' > /var/lib/clickhouse/metadata/broken_db.sql",
        ]
    )
    try:
        # `expected_to_fail` raises if the process is still alive after the timeout, so a hang in
        # the cleanup path fails the test.
        node_fail.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

        # The server rotates the log file on every start, so line offsets taken before the restart
        # are meaningless; slice from the last startup banner instead (`tac`+`sed` prints the lines
        # from the last match to the end of the file). `expected_to_fail` can also return before the
        # started process is even visible, so wait until the failure has made it to the log.
        failed_startup_log = ""
        for _ in range(120):
            failed_startup_log = node_fail.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"tac {LOG_FILE} | sed '/Application: Starting ClickHouse/q' | tac",
                ]
            )
            if "broken_db" in failed_startup_log and node_fail.get_process_pid("clickhouse") is None:
                break
            time.sleep(1)

        assert node_fail.get_process_pid("clickhouse") is None
        # The failed startup got far enough to bind the early Prometheus listener...
        assert "Listening for Prometheus" in failed_startup_log
        # ...and exited because of the metadata loading error rather than anything else.
        assert "broken_db" in failed_startup_log
    finally:
        # Restore the instance so the module teardown is healthy.
        node_fail.exec_in_container(["bash", "-c", "rm -f /var/lib/clickhouse/metadata/broken_db.sql"])
        node_fail.start_clickhouse()
