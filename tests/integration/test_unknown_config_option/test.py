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

# Escape-hatch case: `skip_check_for_incorrect_settings` must disable the new
# unknown-key check from every supported source, including the command line.
# This node carries the very same unknown top-level key that makes `node_bad`
# fail to start, but is launched with `--skip_check_for_incorrect_settings=1`
# on the command line. It must start (and survive `SYSTEM RELOAD CONFIG`)
# exactly the way the command-line flag already disables the pre-existing
# top-level user-setting check. `stay_alive` so the reload step can run.
cluster_cli_skip = ClickHouseCluster(__file__, name="cli_skip")
node_cli_skip = cluster_cli_skip.add_instance(
    "node_cli_skip",
    main_configs=["configs/config.d/unknown_option.xml"],
    extra_args="--skip_check_for_incorrect_settings=1",
    stay_alive=True,
)

# Compatibility case: a `GraphiteMergeTree` rollup config section can have an
# arbitrary name (taken from the table definition, e.g. `GraphiteMergeTree('retention_5m')`),
# not necessarily one starting with `graphite_rollup`. Such a section was valid before this
# check existed, so it must still be accepted (recognized by its rollup structure) and usable.
cluster_graphite = ClickHouseCluster(__file__, name="graphite")
node_graphite = cluster_graphite.add_instance(
    "node_graphite",
    main_configs=["configs/config.d/graphite_arbitrary_name.xml"],
)

# Negative case: an unknown top-level key must NOT be accepted merely because an external
# `<include_from>` substitution source (one that is NOT merged into the config, i.e. lives
# outside `config.d/`) happens to define a tag of the same name. Such a source is only a
# lookup table for `incl` references and contributes no top-level key to the merged config.
cluster_include_from_external = ClickHouseCluster(__file__, name="include_from_external")
node_include_from_external = cluster_include_from_external.add_instance(
    "node_include_from_external",
    main_configs=["configs/config.d/include_from_external_initial.xml"],
    stay_alive=True,
)

# Negative case: a non-static handler (e.g. `redirect`) ignores `response_content`. Only a
# `static` handler consumes a `config://` reference, so the validator must NOT exempt a top-level
# key referenced from `response_content` on a non-static handler — doing so would let a genuinely
# unknown section pass validation (a false negative).
cluster_non_static_ref = ClickHouseCluster(__file__, name="non_static_ref")
node_non_static_ref = cluster_non_static_ref.add_instance(
    "node_non_static_ref",
    main_configs=["configs/config.d/non_static_handler_config_ref.xml"],
)
caught_non_static_ref_exception = ""

# Negative case: `StaticRequestHandler` reads its `config://` payload only from `response_content`,
# never from `response_expression`. A `config://` reference in the unused `response_expression`
# field is ignored by the server, so the validator must NOT exempt the referenced top-level key.
cluster_static_wrong_field = ClickHouseCluster(__file__, name="static_wrong_field")
node_static_wrong_field = cluster_static_wrong_field.add_instance(
    "node_static_wrong_field",
    main_configs=["configs/config.d/static_handler_response_expression.xml"],
)
caught_static_wrong_field_exception = ""

# Positive case: an HTTP protocol endpoint whose `<handlers>` value is a *nested* config prefix
# (`custom_nested.handlers`), not a top-level section. The validator must exempt the top-level
# component (`custom_nested`) AND scan the full nested prefix for `config://` references so the
# referenced top-level key (`nested_payload`) is exempted too.
cluster_protocols_nested = ClickHouseCluster(__file__, name="protocols_nested")
node_protocols_nested = cluster_protocols_nested.add_instance(
    "node_protocols_nested",
    main_configs=["configs/config.d/protocols_nested_handlers.xml"],
)

# Compatibility case: a `GraphiteMergeTree` rollup section may override only the column names and
# define no `<pattern>`/`<default>` rollup rules. `setGraphitePatternsFromConfig` accepts that, so
# the validator must too (recognizing the section by its column-name overrides).
cluster_graphite_columns = ClickHouseCluster(__file__, name="graphite_columns")
node_graphite_columns = cluster_graphite_columns.add_instance(
    "node_graphite_columns",
    main_configs=["configs/config.d/graphite_columns_only.xml"],
)

# Negative case: a section shaped like a `GraphiteMergeTree` rollup (it carries a real `<pattern>`
# rule) but that also contains a foreign child must be rejected. `setGraphitePatternsFromConfig`
# throws `UNKNOWN_ELEMENT_IN_CONFIG` on any unrecognized child, so the validator must require EVERY
# child to be a recognized graphite key — accepting the section just because one child looks like a
# rollup rule would let a typo'd top-level section sneak through (a false negative).
cluster_graphite_foreign = ClickHouseCluster(__file__, name="graphite_foreign")
node_graphite_foreign = cluster_graphite_foreign.add_instance(
    "node_graphite_foreign",
    main_configs=["configs/config.d/graphite_foreign_child.xml"],
)
caught_graphite_foreign_exception = ""

# Negative case: a `config://` payload section that happens to contain handler-shaped children must
# NOT be re-scanned as a handler group. The unrelated top-level key referenced from those nested
# children (`secret_unknown_section`) must still be rejected.
cluster_payload_handler_shape = ClickHouseCluster(__file__, name="payload_handler_shape")
node_payload_handler_shape = cluster_payload_handler_shape.add_instance(
    "node_payload_handler_shape",
    main_configs=["configs/config.d/config_payload_handler_shape.xml"],
)
caught_payload_handler_shape_exception = ""

# Invariant case: a *failed* reload must leave the live layered config unchanged. The node starts
# serving a known `config://` HTTP response; a bad reload (which changes that response AND adds an
# unknown key) must be rejected, and the original response must still be served afterwards.
cluster_reload_invariant = ClickHouseCluster(__file__, name="reload_invariant")
node_reload_invariant = cluster_reload_invariant.add_instance(
    "node_reload_invariant",
    main_configs=["configs/config.d/reload_invariant_initial.xml"],
    stay_alive=True,
)

# Reload regression: a `<skip_check_for_incorrect_settings>` flag supplied by a *previously loaded*
# config file must not leak into a later reload decision. Starting with the flag set in a file, a
# reload that removes the flag while adding an unknown top-level key must be rejected (matching a
# fresh startup with the same file).
cluster_reload_skip_state = ClickHouseCluster(__file__, name="reload_skip_state")
node_reload_skip_state = cluster_reload_skip_state.add_instance(
    "node_reload_skip_state",
    main_configs=["configs/config.d/reload_skip_state_initial.xml"],
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


@pytest.fixture(scope="module")
def start_cli_skip_cluster():
    cluster_cli_skip.start()
    yield
    cluster_cli_skip.shutdown()


@pytest.fixture(scope="module")
def start_graphite_cluster():
    cluster_graphite.start()
    yield
    cluster_graphite.shutdown()


@pytest.fixture(scope="module")
def start_include_from_external_cluster():
    cluster_include_from_external.start()
    yield
    cluster_include_from_external.shutdown()


@pytest.fixture(scope="module")
def start_non_static_ref_cluster():
    global caught_non_static_ref_exception
    try:
        cluster_non_static_ref.start()
    except Exception as e:
        caught_non_static_ref_exception = str(e)
        err_log = os.path.join(node_non_static_ref.logs_dir, "clickhouse-server.err.log")
        if os.path.exists(err_log):
            with open(err_log, "r") as f:
                caught_non_static_ref_exception += "\n" + f.read()
    yield
    cluster_non_static_ref.shutdown()


@pytest.fixture(scope="module")
def start_static_wrong_field_cluster():
    global caught_static_wrong_field_exception
    try:
        cluster_static_wrong_field.start()
    except Exception as e:
        caught_static_wrong_field_exception = str(e)
        err_log = os.path.join(
            node_static_wrong_field.logs_dir, "clickhouse-server.err.log"
        )
        if os.path.exists(err_log):
            with open(err_log, "r") as f:
                caught_static_wrong_field_exception += "\n" + f.read()
    yield
    cluster_static_wrong_field.shutdown()


@pytest.fixture(scope="module")
def start_protocols_nested_cluster():
    cluster_protocols_nested.start()
    yield
    cluster_protocols_nested.shutdown()


@pytest.fixture(scope="module")
def start_graphite_columns_cluster():
    cluster_graphite_columns.start()
    yield
    cluster_graphite_columns.shutdown()


@pytest.fixture(scope="module")
def start_graphite_foreign_cluster():
    global caught_graphite_foreign_exception
    try:
        cluster_graphite_foreign.start()
    except Exception as e:
        caught_graphite_foreign_exception = str(e)
        err_log = os.path.join(
            node_graphite_foreign.logs_dir, "clickhouse-server.err.log"
        )
        if os.path.exists(err_log):
            with open(err_log, "r") as f:
                caught_graphite_foreign_exception += "\n" + f.read()
    yield
    cluster_graphite_foreign.shutdown()


@pytest.fixture(scope="module")
def start_payload_handler_shape_cluster():
    global caught_payload_handler_shape_exception
    try:
        cluster_payload_handler_shape.start()
    except Exception as e:
        caught_payload_handler_shape_exception = str(e)
        err_log = os.path.join(
            node_payload_handler_shape.logs_dir, "clickhouse-server.err.log"
        )
        if os.path.exists(err_log):
            with open(err_log, "r") as f:
                caught_payload_handler_shape_exception += "\n" + f.read()
    yield
    cluster_payload_handler_shape.shutdown()


@pytest.fixture(scope="module")
def start_reload_invariant_cluster():
    cluster_reload_invariant.start()
    yield
    cluster_reload_invariant.shutdown()


@pytest.fixture(scope="module")
def start_reload_skip_state_cluster():
    cluster_reload_skip_state.start()
    yield
    cluster_reload_skip_state.shutdown()


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


def test_non_static_handler_config_ref_not_exempted(start_non_static_ref_cluster):
    # A `redirect` handler ignores `response_content`, so `config://redirect_ignored_payload`
    # is never consumed by any server code. The unknown top-level key must still be rejected
    # (only a `static` handler may exempt a `config://`-referenced key).
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in caught_non_static_ref_exception
    assert "redirect_ignored_payload" in caught_non_static_ref_exception


def test_static_handler_response_expression_field_not_exempted(
    start_static_wrong_field_cluster,
):
    # `StaticRequestHandler` reads `response_content`, not `response_expression`; a `config://`
    # reference in the unused field must not exempt the unknown top-level key.
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in caught_static_wrong_field_exception
    assert "static_response_expression_payload" in caught_static_wrong_field_exception


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


def test_cli_skip_flag_disables_check(start_cli_skip_cluster):
    # Startup coverage: `node_cli_skip` carries the same `some_completely_unknown_option`
    # top-level key that makes `node_bad` fail to start, but it is launched with
    # `--skip_check_for_incorrect_settings=1`. The command-line escape hatch (resolved
    # from the layered config) must disable the unknown-key check, so the node starts.
    assert node_cli_skip.query("SELECT 1").strip() == "1"

    # Reload coverage: the command-line flag persists in the layered config across
    # `SYSTEM RELOAD CONFIG`, so injecting another unknown top-level key and reloading
    # must NOT raise `UNKNOWN_ELEMENT_IN_CONFIG`.
    extra_unknown_path = "/etc/clickhouse-server/config.d/cli_skip_unknown.xml"
    extra_unknown = (
        "<clickhouse>"
        "<another_completely_unknown_option>1</another_completely_unknown_option>"
        "</clickhouse>"
    )
    try:
        node_cli_skip.replace_config(extra_unknown_path, extra_unknown)
        # `query` raises on error; a clean return proves the reload was accepted.
        node_cli_skip.query("SYSTEM RELOAD CONFIG")
        assert node_cli_skip.query("SELECT 1").strip() == "1"
    finally:
        node_cli_skip.exec_in_container(["bash", "-c", f"rm -f {extra_unknown_path}"])


def test_cli_skip_flag_disables_user_setting_check_on_reload(start_cli_skip_cluster):
    # Regression for the command-line escape hatch on `SYSTEM RELOAD CONFIG` for the
    # *pre-existing* top-level user-setting check (`Settings::checkNoSettingNamesAtTopLevel`),
    # not only for the new unknown-server-key check. On reload that helper validates the
    # file-only config (so a failed reload does not mutate the layered config), which does not
    # carry command-line options; the escape hatch must therefore be resolved from the layered
    # config. Injecting a top-level user setting such as `<max_memory_usage>` and reloading must
    # NOT raise `UNKNOWN_ELEMENT_IN_CONFIG`, exactly as the command-line flag suppresses that
    # same check at startup.
    user_setting_path = "/etc/clickhouse-server/config.d/cli_skip_user_setting.xml"
    user_setting = (
        "<clickhouse>"
        "<max_memory_usage>1</max_memory_usage>"
        "</clickhouse>"
    )
    try:
        node_cli_skip.replace_config(user_setting_path, user_setting)
        # `query` raises on error; a clean return proves the reload was accepted.
        node_cli_skip.query("SYSTEM RELOAD CONFIG")
        assert node_cli_skip.query("SELECT 1").strip() == "1"
    finally:
        node_cli_skip.exec_in_container(["bash", "-c", f"rm -f {user_setting_path}"])


def test_graphite_rollup_arbitrary_section_name_accepted(start_graphite_cluster):
    # A `GraphiteMergeTree` rollup section can have an arbitrary name (taken from the table
    # definition, here `retention_5m`), not necessarily one starting with `graphite_rollup`.
    # If the unknown-key validator rejected `<retention_5m>`, the node would have failed to
    # start. The section must also remain usable: a `GraphiteMergeTree('retention_5m')` table
    # is created, populated, and rolled up.
    node_graphite.query("DROP TABLE IF EXISTS test_graphite SYNC")
    node_graphite.query(
        """
        CREATE TABLE test_graphite
            (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
            ENGINE = GraphiteMergeTree('retention_5m')
            PARTITION BY toYYYYMM(date)
            ORDER BY (metric, timestamp)
        """
    )
    node_graphite.query(
        "INSERT INTO test_graphite VALUES ('metric1', 1.0, 1, toDate('2020-01-01'), 1)"
    )
    node_graphite.query("OPTIMIZE TABLE test_graphite FINAL")
    assert node_graphite.query("SELECT count() FROM test_graphite").strip() == "1"
    node_graphite.query("DROP TABLE test_graphite SYNC")


def test_external_include_from_source_does_not_exempt_unknown_key(
    start_include_from_external_cluster,
):
    # An external `<include_from>` source that lives OUTSIDE `config.d/` is used by
    # `ConfigProcessor` only as a lookup table for `incl` references; it does not contribute
    # any top-level key to the merged config. Therefore an unknown top-level key must still be
    # rejected even when the external source happens to define a tag of the same name.
    # (Before the fix, the validator exempted every top-level tag of every `include_from`
    # source unconditionally, masking exactly this typo/misplaced-section class.)
    external_source_path = "/etc/clickhouse-server/external_incl_source.xml"
    external_source = (
        "<clickhouse>"
        "<my_external_only_payload>lookup value</my_external_only_payload>"
        "</clickhouse>"
    )
    # The unknown top-level key shares its name with the external source's tag, so it would be
    # wrongly exempted by the old code. It is paired with the `<include_from>` directive that
    # points at the external (non-merged) source.
    bad_config_path = "/etc/clickhouse-server/config.d/external_include_from.xml"
    bad_config = (
        "<clickhouse>"
        f"<include_from>{external_source_path}</include_from>"
        "<my_external_only_payload>1</my_external_only_payload>"
        "</clickhouse>"
    )
    try:
        node_include_from_external.replace_config(external_source_path, external_source)
        node_include_from_external.replace_config(bad_config_path, bad_config)
        error = node_include_from_external.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "UNKNOWN_ELEMENT_IN_CONFIG" in error
        assert "my_external_only_payload" in error
    finally:
        node_include_from_external.exec_in_container(
            ["bash", "-c", f"rm -f {bad_config_path} {external_source_path}"]
        )


def test_protocols_nested_handler_prefix_accepted(start_protocols_nested_cluster):
    # The endpoint references the *nested* handler prefix `custom_nested.handlers`. The validator
    # must exempt the top-level component `custom_nested` (otherwise the node rejects `<custom_nested>`)
    # and must scan the full nested prefix for `config://` references (otherwise the node rejects the
    # `<nested_payload>` referenced from the nested static handler). If either failed, the node would
    # not have started.
    assert node_protocols_nested.query("SELECT 1").strip() == "1"


def test_graphite_columns_only_section_accepted(start_graphite_columns_cluster):
    # A `GraphiteMergeTree` rollup section that only overrides column names (no `<pattern>`/`<default>`)
    # is a valid config accepted by `setGraphitePatternsFromConfig`. If the validator rejected
    # `<retention_columns_only>` for lacking a rollup rule, the node would have failed to start.
    assert node_graphite_columns.query("SELECT 1").strip() == "1"


def test_graphite_shaped_section_with_foreign_child_rejected(
    start_graphite_foreign_cluster,
):
    # `<graphite_with_foreign_child>` carries a real `<pattern>` rollup rule (so it *looks* like a
    # `GraphiteMergeTree` section) but also has a `<not_a_graphite_key>` child that
    # `setGraphitePatternsFromConfig` would reject. The validator must mirror that parser and reject
    # the whole section as an unknown top-level key, instead of accepting it because one child looks
    # like a rollup rule. The reported unknown element is the top-level section name.
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in caught_graphite_foreign_exception
    assert "graphite_with_foreign_child" in caught_graphite_foreign_exception


def test_payload_with_handler_shape_does_not_exempt_unknown_key(
    start_payload_handler_shape_cluster,
):
    # `<config_payload>` is exempt because `<http_handlers>` references it via `config://`, but it is
    # only a static-response payload, not a handler group. The validator must not re-scan it as a
    # handler group, so the unrelated top-level `<secret_unknown_section>` referenced from its nested
    # children must still be rejected. The node must fail to start with `UNKNOWN_ELEMENT_IN_CONFIG`.
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in caught_payload_handler_shape_exception
    assert "secret_unknown_section" in caught_payload_handler_shape_exception


def test_failed_reload_leaves_live_config_unchanged(start_reload_invariant_cluster):
    # A failed reload must be rejected BEFORE the new config is installed, so the live config is
    # left untouched. The node serves a known `config://` response; a bad reload changes that
    # response and adds an unknown key. After the rejected reload, the ORIGINAL response must still
    # be served (proving the failed reload did not mutate the live layered config).
    response = node_reload_invariant.http_request("reload_invariant", method="GET")
    assert response.status_code == 200
    assert response.text == "original payload"

    initial_config_path = (
        "/etc/clickhouse-server/config.d/reload_invariant_initial.xml"
    )
    original_config = (
        "<clickhouse>"
        "<reload_invariant_payload>original payload</reload_invariant_payload>"
        "<http_handlers>"
        "<rule>"
        "<methods>GET</methods>"
        "<url>/reload_invariant</url>"
        "<handler>"
        "<type>static</type>"
        "<response_content>config://reload_invariant_payload</response_content>"
        "</handler>"
        "</rule>"
        "<defaults/>"
        "</http_handlers>"
        "</clickhouse>"
    )
    # The bad reload changes the served payload AND adds an unknown top-level key, so the reload
    # must be rejected; the changed payload must NOT take effect.
    bad_config = (
        "<clickhouse>"
        "<reload_invariant_payload>changed payload</reload_invariant_payload>"
        "<reload_invariant_unknown_option>1</reload_invariant_unknown_option>"
        "<http_handlers>"
        "<rule>"
        "<methods>GET</methods>"
        "<url>/reload_invariant</url>"
        "<handler>"
        "<type>static</type>"
        "<response_content>config://reload_invariant_payload</response_content>"
        "</handler>"
        "</rule>"
        "<defaults/>"
        "</http_handlers>"
        "</clickhouse>"
    )
    try:
        node_reload_invariant.replace_config(initial_config_path, bad_config)
        assert "UNKNOWN_ELEMENT_IN_CONFIG" in node_reload_invariant.query_and_get_error(
            "SYSTEM RELOAD CONFIG"
        )
        # The live config must be unchanged: the original response is still served.
        response = node_reload_invariant.http_request("reload_invariant", method="GET")
        assert response.status_code == 200
        assert response.text == "original payload"
    finally:
        node_reload_invariant.replace_config(initial_config_path, original_config)
        node_reload_invariant.query("SYSTEM RELOAD CONFIG")


def test_reload_does_not_inherit_stale_skip_flag(start_reload_skip_state_cluster):
    # The node starts with `<skip_check_for_incorrect_settings>1</skip_check_for_incorrect_settings>`
    # supplied by a config file (not the command line). A reload that REMOVES that flag while adding
    # an unknown top-level key must be rejected: the previously loaded file's `skip` value must not
    # leak into the reload decision. Before the fix, the reload read the stale layered `skip=1` and
    # wrongly accepted the invalid config.
    assert node_reload_skip_state.query("SELECT 1").strip() == "1"

    initial_config_path = (
        "/etc/clickhouse-server/config.d/reload_skip_state_initial.xml"
    )
    original_config = (
        "<clickhouse>"
        "<skip_check_for_incorrect_settings>1</skip_check_for_incorrect_settings>"
        "</clickhouse>"
    )
    # The flag is gone and an unknown key is present, so the reload must be rejected.
    bad_config = (
        "<clickhouse>"
        "<stale_skip_unknown_option>1</stale_skip_unknown_option>"
        "</clickhouse>"
    )
    try:
        node_reload_skip_state.replace_config(initial_config_path, bad_config)
        error = node_reload_skip_state.query_and_get_error("SYSTEM RELOAD CONFIG")
        assert "UNKNOWN_ELEMENT_IN_CONFIG" in error
        assert "stale_skip_unknown_option" in error
    finally:
        node_reload_skip_state.replace_config(initial_config_path, original_config)
        node_reload_skip_state.query("SYSTEM RELOAD CONFIG")
