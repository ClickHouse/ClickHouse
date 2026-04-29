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

# Reload regression: the unknown-key check also runs on `SYSTEM RELOAD CONFIG`,
# not only at startup. Use a separate cluster that starts with a minimal
# placeholder config (no `<http_handlers>`) so that the test can later inject
# `<http_handlers>` via a `config.d/` file without colliding with merged rules
# from another file.
cluster_reload = ClickHouseCluster(__file__, name="reload")
node_reload = cluster_reload.add_instance(
    "node_reload",
    main_configs=["configs/config.d/reload_initial.xml"],
    stay_alive=True,
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
def start_reload_cluster():
    cluster_reload.start()
    yield
    cluster_reload.shutdown()


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


def test_reload_rejects_unknown_then_accepts_config_ref(start_reload_cluster):
    # The node started with a valid config; the validator must also run on
    # `SYSTEM RELOAD CONFIG`, not only at startup.
    assert node_reload.query("SELECT 1").strip() == "1"

    bad_config_path = "/etc/clickhouse-server/config.d/reload_unknown.xml"
    bad_config = (
        "<clickhouse>"
        "<some_other_unknown_option>1</some_other_unknown_option>"
        "</clickhouse>"
    )
    good_config_path = "/etc/clickhouse-server/config.d/reload_payload.xml"
    good_config = (
        "<clickhouse>"
        "<my_reload_payload>Hello after reload</my_reload_payload>"
        "<http_handlers>"
        "<rule>"
        "<methods>GET</methods>"
        "<url>/my_reload_response</url>"
        "<handler>"
        "<type>static</type>"
        "<response_content>config://my_reload_payload</response_content>"
        "</handler>"
        "</rule>"
        "<defaults/>"
        "</http_handlers>"
        "</clickhouse>"
    )

    try:
        # Step 1: write an unknown top-level key into config.d and reload.
        # `SYSTEM RELOAD CONFIG` must surface `UNKNOWN_ELEMENT_IN_CONFIG`.
        node_reload.replace_config(bad_config_path, bad_config)
        assert "UNKNOWN_ELEMENT_IN_CONFIG" in node_reload.query_and_get_error(
            "SYSTEM RELOAD CONFIG"
        )

        # Step 2: replace the bad file with a valid `config://`-referenced key
        # and reload again. The validator must accept the new top-level key.
        node_reload.exec_in_container(
            ["bash", "-c", f"rm -f {bad_config_path}"]
        )
        node_reload.replace_config(good_config_path, good_config)
        node_reload.query("SYSTEM RELOAD CONFIG")
        response = node_reload.http_request("my_reload_response", method="GET")
        assert response.status_code == 200
        assert response.text == "Hello after reload"
    finally:
        node_reload.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -f {bad_config_path} {good_config_path}",
            ]
        )
