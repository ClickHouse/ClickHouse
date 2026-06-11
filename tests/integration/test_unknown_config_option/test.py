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

# Negative case: a typo of a real `users_*` key (e.g. `<users_cnfig>` instead of
# `<users_config>`) must NOT pass through a blanket `users_*` prefix allowlist.
cluster_users_typo = ClickHouseCluster(__file__, name="users_typo")
node_users_typo = cluster_users_typo.add_instance(
    "node_users_typo",
    main_configs=["configs/config.d/users_typo.xml"],
)
caught_users_typo_exception = ""

# Positive case: a custom top-level key referenced via config:// from an
# HTTP handler must be accepted without setting skip_check_for_incorrect_settings.
cluster_ok = ClickHouseCluster(__file__, name="ok")
node_static_config_ref = cluster_ok.add_instance(
    "node_static_config_ref",
    main_configs=["configs/config.d/static_handler_config_ref.xml"],
)

# Positive case: a `config://` reference can carry Poco's bracket-index path
# syntax (e.g. `my_indexed_payload[1]` to address the second instance of a
# repeated top-level key). The validator must normalize the recorded key by
# stripping the bracket suffix so the corresponding `top_level_keys` (which
# Poco yields with `[N]` suffixes) match.
cluster_indexed_ref = ClickHouseCluster(__file__, name="indexed_ref")
node_static_config_ref_indexed = cluster_indexed_ref.add_instance(
    "node_static_config_ref_indexed",
    main_configs=["configs/config.d/static_handler_config_ref_indexed.xml"],
)

# Positive case: a custom top-level handlers section referenced via
# <protocols>...<handlers>NAME</handlers>...</protocols> must be accepted.
cluster_protocols = ClickHouseCluster(__file__, name="protocols")
node_protocols_custom_handlers = cluster_protocols.add_instance(
    "node_protocols_custom_handlers",
    main_configs=["configs/config.d/protocols_custom_handlers.xml"],
)

# Positive case: when the `include_from` source file lives under `config.d/`,
# `ConfigProcessor` merges its top-level tags into the main config. Those tags
# are pure substitution sources (referenced via `<elem incl="name"/>`) and must
# not be rejected by the unknown-key check.
cluster_include_from_in_configd = ClickHouseCluster(__file__, name="include_from_in_configd")
node_include_from_in_configd = cluster_include_from_in_configd.add_instance(
    "node_include_from_in_configd",
    main_configs=[
        "configs/config.d/include_from_main.xml",
        "configs/config.d/include_from_source.xml",
    ],
)

# Positive case: top-level keys that are consumed outside `ServerSettings`
# (read directly from the server config by `TCPHandler`, `HTTPHandler`,
# `TablesLoader`, bridges, etc.) must be accepted. Regression coverage so that
# future allowlist edits don't reject configs that were valid before.
cluster_existing_keys = ClickHouseCluster(__file__, name="existing_keys")
node_existing_keys = cluster_existing_keys.add_instance(
    "node_existing_keys",
    main_configs=["configs/config.d/existing_keys.xml"],
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
def start_users_typo_cluster():
    global caught_users_typo_exception
    try:
        cluster_users_typo.start()
    except Exception as e:
        caught_users_typo_exception = str(e)
        err_log = os.path.join(node_users_typo.logs_dir, "clickhouse-server.err.log")
        if os.path.exists(err_log):
            with open(err_log, "r") as f:
                caught_users_typo_exception += "\n" + f.read()
    yield
    cluster_users_typo.shutdown()


@pytest.fixture(scope="module")
def start_ok_cluster():
    cluster_ok.start()
    yield
    cluster_ok.shutdown()


@pytest.fixture(scope="module")
def start_indexed_ref_cluster():
    cluster_indexed_ref.start()
    yield
    cluster_indexed_ref.shutdown()


@pytest.fixture(scope="module")
def start_protocols_cluster():
    cluster_protocols.start()
    yield
    cluster_protocols.shutdown()


@pytest.fixture(scope="module")
def start_include_from_in_configd_cluster():
    cluster_include_from_in_configd.start()
    yield
    cluster_include_from_in_configd.shutdown()


@pytest.fixture(scope="module")
def start_existing_keys_cluster():
    cluster_existing_keys.start()
    yield
    cluster_existing_keys.shutdown()


@pytest.fixture(scope="module")
def start_reload_cluster():
    cluster_reload.start()
    yield
    cluster_reload.shutdown()


def test_unknown_config_option_rejected(start_bad_cluster):
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in caught_exception
    assert "some_completely_unknown_option" in caught_exception


def test_users_prefix_typo_rejected(start_users_typo_cluster):
    # A typo of `users_config` (e.g. `users_cnfig`) must be rejected: the
    # validator must not blanket-accept any `users_*` top-level key.
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in caught_users_typo_exception
    assert "users_cnfig" in caught_users_typo_exception


def test_config_ref_in_http_handler_accepted(start_ok_cluster):
    # If the unknown-key validator rejected `my_static_response_payload`,
    # the node would have failed to start and the HTTP handler would not respond.
    response = node_static_config_ref.http_request("my_static_response", method="GET")
    assert response.status_code == 200
    assert response.text == "Hello from config://"


def test_config_ref_with_bracket_index_accepted(start_indexed_ref_cluster):
    # If the unknown-key validator failed to normalize `my_indexed_payload[1]`
    # to `my_indexed_payload` before recording it as referenced, the node
    # would have refused to start (the repeated top-level key is yielded by
    # Poco as `my_indexed_payload` and `my_indexed_payload[1]`, both of which
    # must be matched against the normalized reference).
    response = node_static_config_ref_indexed.http_request(
        "my_indexed_response", method="GET"
    )
    assert response.status_code == 200
    assert response.text == "Second payload"


def test_protocols_custom_handlers_accepted(start_protocols_cluster):
    # If the unknown-key validator rejected `my_custom_handlers` (the section
    # referenced by <protocols><alt_http><handlers>my_custom_handlers</handlers>...),
    # the node would have failed to start.
    assert (
        node_protocols_custom_handlers.query("SELECT 1").strip() == "1"
    )


def test_include_from_source_in_configd_accepted(
    start_include_from_in_configd_cluster,
):
    # If the unknown-key validator rejected `my_incl_payload` (which is the
    # top-level tag in the `include_from` source file placed under `config.d/`,
    # and thus auto-merged into the main config by `ConfigProcessor`), the node
    # would have failed to start. The fix is to parse the `include_from` source
    # separately and treat its top-level tag names as referenced (i.e. exempt).
    assert (
        node_include_from_in_configd.query("SELECT 1").strip() == "1"
    )


def test_existing_keys_outside_server_settings_accepted(start_existing_keys_cluster):
    # If the unknown-key validator rejected any of the keys in
    # `existing_keys.xml` (all of which are read by C++ code outside
    # `ServerSettings`), the node would have failed to start.
    assert node_existing_keys.query("SELECT 1").strip() == "1"


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
