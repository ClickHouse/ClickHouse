import os
import pytest

from helpers.cluster import ClickHouseCluster

# Negative case: an entirely unknown top-level key must be rejected.
cluster_bad = ClickHouseCluster(__file__, name="bad")
node_bad = cluster_bad.add_instance(
    "node_bad",
    main_configs=["configs/config.d/unknown_option.xml"],
)
caught_exception = ""

# Positive case: a custom top-level key referenced via config:// from an
# HTTP handler must be accepted without setting skip_check_for_incorrect_settings.
cluster_ok = ClickHouseCluster(__file__, name="ok")
node_static_config_ref = cluster_ok.add_instance(
    "node_static_config_ref",
    main_configs=["configs/config.d/static_handler_config_ref.xml"],
)


@pytest.fixture(scope="module")
def start_bad_cluster():
    global caught_exception
    try:
        cluster_bad.start()
    except Exception as e:
        caught_exception = str(e)
        # The error message goes to the error log file, not to container stdout.
        # Read it from the host-mounted logs directory.
        err_log = os.path.join(node_bad.logs_dir, "clickhouse-server.err.log")
        if os.path.exists(err_log):
            with open(err_log, "r") as f:
                caught_exception += "\n" + f.read()
    yield
    cluster_bad.shutdown()


@pytest.fixture(scope="module")
def start_ok_cluster():
    cluster_ok.start()
    yield
    cluster_ok.shutdown()


def test_unknown_config_option_rejected(start_bad_cluster):
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in caught_exception
    assert "some_completely_unknown_option" in caught_exception


def test_config_ref_in_http_handler_accepted(start_ok_cluster):
    # If the unknown-key validator rejected `my_static_response_payload`,
    # the node would have failed to start and the HTTP handler would not respond.
    response = node_static_config_ref.http_request("my_static_response", method="GET")
    assert response.status_code == 200
    assert response.text == "Hello from config://"
