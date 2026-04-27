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

# Positive case: a custom top-level handlers section referenced via
# <protocols>...<handlers>NAME</handlers>...</protocols> must be accepted.
cluster_protocols = ClickHouseCluster(__file__, name="protocols")
node_protocols_custom_handlers = cluster_protocols.add_instance(
    "node_protocols_custom_handlers",
    main_configs=["configs/config.d/protocols_custom_handlers.xml"],
)

# Positive case: short CLI option keys (C/L/E/P/h/V) injected by `argsToConfig`
# must not be rejected by checkUnknownSettings.
cluster_short_cli = ClickHouseCluster(__file__, name="short_cli")
node_short_cli = cluster_short_cli.add_instance(
    "node_short_cli",
    main_configs=["configs/config.d/cli_short_keys.xml"],
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


@pytest.fixture(scope="module")
def start_protocols_cluster():
    cluster_protocols.start()
    yield
    cluster_protocols.shutdown()


@pytest.fixture(scope="module")
def start_short_cli_cluster():
    cluster_short_cli.start()
    yield
    cluster_short_cli.shutdown()


def test_unknown_config_option_rejected(start_bad_cluster):
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in caught_exception
    assert "some_completely_unknown_option" in caught_exception


def test_config_ref_in_http_handler_accepted(start_ok_cluster):
    # If the unknown-key validator rejected `my_static_response_payload`,
    # the node would have failed to start and the HTTP handler would not respond.
    response = node_static_config_ref.http_request("my_static_response", method="GET")
    assert response.status_code == 200
    assert response.text == "Hello from config://"


def test_protocols_custom_handlers_accepted(start_protocols_cluster):
    # If the unknown-key validator rejected `my_custom_handlers` (the section
    # referenced by <protocols><alt_http><handlers>my_custom_handlers</handlers>...),
    # the node would have failed to start.
    assert (
        node_protocols_custom_handlers.query("SELECT 1").strip() == "1"
    )


def test_short_cli_option_keys_accepted(start_short_cli_cluster):
    # If the unknown-key validator rejected the single-letter keys C/L/E/P/h/V
    # that argsToConfig injects for short CLI options, the node would have
    # failed to start.
    assert node_short_cli.query("SELECT 1").strip() == "1"
