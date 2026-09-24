"""
Guards the hard-coded tables of `tests/integration/helpers/ci_logs_export.py`
against silent rot.

The helper decides per instance whether to export its system log tables, and it
must answer "does this container run the binary under test?". That cannot be
probed before the container starts, so the helper carries an explicit map of the
integration-test Docker images to the environment variable holding the tag built
for the current commit. Two ways for that map to go stale, both silent - the
suites just stop exporting logs, and nothing turns red:

  - a new image derived from `clickhouse/integration-test` is added (the way
    `clickhouse/integration-test-with-unity-catalog` was) and not listed here;
  - the environment variable of a listed image is renamed in `IMAGES_ENV`, so the
    tag never matches.

The `_watcher` materialized views also run as `ci_logs_sender`, whose profile
pins the export settings (short timeouts, async inserts, no cache pollution)
regardless of the settings of the query that triggered the view. The user comes
from the config the functional tests install, which the base server config does
not ship, so its absence would only show up as a per-table warning in the pytest
log.

See ClickHouse/ClickHouse#116031 (review).
"""

import importlib.util
import os
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "ci"))

from ci.defs.defs import DOCKERS
from ci.jobs.scripts import log_cluster, log_export
from ci.jobs.scripts.integration_tests_configs import IMAGES_ENV
from ci.jobs.scripts.log_cluster import LogCluster

BASE_IMAGE = "clickhouse/integration-test"


def _load_helper():
    """Load the helper by path: importing it as `helpers.ci_logs_export` would
    put the integration-test `helpers` package on the path of the whole run, and
    `helpers.cluster` needs third-party modules this job does not have."""
    path = REPO_ROOT / "tests" / "integration" / "helpers" / "ci_logs_export.py"
    spec = importlib.util.spec_from_file_location("_ci_logs_export", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HELPER = _load_helper()


def _images_running_the_binary_under_test():
    """The integration-test images whose containers run the ClickHouse binary of
    the commit under test: the base image and every image whose Dockerfile is
    `FROM clickhouse/integration-test`."""
    images = {BASE_IMAGE}
    for docker in DOCKERS:
        dockerfile = REPO_ROOT / docker.path / "Dockerfile"
        if not dockerfile.exists():
            continue
        for line in dockerfile.read_text().splitlines():
            match = re.match(r"\s*FROM\s+(\S+)", line, re.IGNORECASE)
            if match and match.group(1).split(":")[0] == BASE_IMAGE:
                images.add(docker.name)
    return images


def test_every_current_binary_image_is_listed():
    assert (
        HELPER.CURRENT_BINARY_IMAGE_TAG_ENV.keys()
        == _images_running_the_binary_under_test()
    )


@pytest.mark.parametrize("image", sorted(HELPER.CURRENT_BINARY_IMAGE_TAG_ENV))
def test_tag_environment_variable_matches_images_env(image):
    assert HELPER.CURRENT_BINARY_IMAGE_TAG_ENV[image] == IMAGES_ENV[image]


def test_eligibility_follows_the_tag_of_the_image(monkeypatch):
    monkeypatch.setenv("DOCKER_BASE_TAG", "abcdef")
    monkeypatch.setenv("DOCKER_BASE_WITH_UNITY_CATALOG_TAG", "123456")
    assert HELPER.runs_binary_under_test(BASE_IMAGE, "abcdef")
    assert not HELPER.runs_binary_under_test(BASE_IMAGE, "123456")
    assert HELPER.runs_binary_under_test(
        "clickhouse/integration-test-with-unity-catalog", "123456"
    )
    # An old release image, and a mock HTTP service that is not a ClickHouse server
    assert not HELPER.runs_binary_under_test("clickhouse/clickhouse-server", "abcdef")
    assert not HELPER.runs_binary_under_test("clickhouse/python-bottle", "abcdef")


def test_sender_user_config_is_the_one_functional_tests_install():
    config = Path(HELPER.SENDER_USER_CONFIG).read_text()
    assert os.path.basename(HELPER.SENDER_USER_CONFIG) == "ci_logs_sender.yaml"
    assert f"    {HELPER.SENDER_USER}:" in config
    assert "constraints:" in config
    assert "async_insert: 1" in config


def _extra_column_names(extra_columns):
    """The names of the columns declared by an EXTRA_COLUMNS string, in order,
    without the index declarations."""
    names = []
    for item in extra_columns.split(","):
        item = item.strip()
        if not item or item.startswith("INDEX "):
            continue
        names.append(item.split()[0])
    return names


def _aliases(expression):
    """The aliases of a SELECT expression list, in order. Only the alias of the
    whole element counts, so that the `AS` of a nested `CAST(x AS UInt32)` is not
    mistaken for one."""
    elements = []
    depth = 0
    current = []
    for char in expression:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            elements.append("".join(current))
            current = []
        else:
            current.append(char)
    elements.append("".join(current))
    names = []
    for element in elements:
        found = re.findall(r"\bAS (\w+)", element)
        assert found, f"no alias in {element!r}"
        names.append(found[-1])
    return names


class _FakeInfo:
    """The CI run identity `log_export` builds the expression from."""

    repo_name = "ClickHouse/ClickHouse"
    pr_number = 0
    sha = "0" * 40
    job_name = "Some job"
    instance_type = "c5.large"
    instance_id = "i-01234567"
    # A Unix timestamp, like the one the config job resolves
    workflow_start_time = 1767225600


SETUP_LOG_CLUSTER = (
    REPO_ROOT / "ci" / "jobs" / "scripts" / "functional_tests" / "setup_log_cluster.sh"
)


def _shell_default(variable):
    """The default value of a `VAR=${VAR:-"..."}` assignment in
    setup_log_cluster.sh."""
    match = re.search(
        r"^\s*" + variable + r"=\$\{" + variable + r':-"(.*)"\}$',
        SETUP_LOG_CLUSTER.read_text(),
        re.MULTILINE,
    )
    assert match, f"no default of {variable} in {SETUP_LOG_CLUSTER}"
    return match.group(1)


def test_destination_structure_is_shared_with_the_functional_tests():
    """The structure hash of a destination table is computed from the columns,
    so the functional and the integration tests only share a table while these
    two declarations are identical. The functional tests build `EXTRA_COLUMNS`
    from `LogCluster.META_COLUMNS`; the helper cannot import it (it runs in the
    test runner container) and repeats it."""
    assert LogCluster.extra_columns_ddl() == HELPER.EXTRA_COLUMNS
    assert _shell_default("EXTRA_ORDER_BY_COLUMNS") == HELPER.EXTRA_ORDER_BY_COLUMNS


def test_job_expression_follows_the_column_order(monkeypatch):
    """A `SELECT {expression}, *` in a different order than EXTRA_COLUMNS gives
    the local sender table a different header than the destination table, so
    `Distributed` converts every exported batch by name and logs a warning for
    each of them - which `system.text_log` then exports as well."""
    monkeypatch.setattr(log_export, "Info", _FakeInfo)
    monkeypatch.setattr(log_cluster, "Info", _FakeInfo)
    assert _aliases(
        LogCluster.extra_columns_expression("2026-01-01 00:00:00")
    ) == _extra_column_names(HELPER.EXTRA_COLUMNS)


def test_helper_expression_follows_the_column_order(monkeypatch):
    """The integration tests take the expression in two parts and insert the
    per-server `test_name` and `node_name` between them."""
    monkeypatch.setattr(log_export, "Info", _FakeInfo)
    monkeypatch.setattr(log_cluster, "Info", _FakeInfo)
    monkeypatch.delenv(HELPER.EXTRA_COLUMNS_EXPRESSION_HEAD_ENV, raising=False)
    monkeypatch.delenv(HELPER.EXTRA_COLUMNS_EXPRESSION_TAIL_ENV, raising=False)
    expected = _extra_column_names(HELPER.EXTRA_COLUMNS)
    # The default expression of a local run
    assert _aliases(HELPER._extra_columns_expression("test", "node")) == expected
    # And the one built from the values the CI job provides
    monkeypatch.setenv(
        HELPER.EXTRA_COLUMNS_EXPRESSION_HEAD_ENV,
        log_export.extra_columns_expression_head(0),
    )
    monkeypatch.setenv(
        HELPER.EXTRA_COLUMNS_EXPRESSION_TAIL_ENV,
        log_export.extra_columns_expression_tail(),
    )
    assert _aliases(HELPER._extra_columns_expression("test", "node")) == expected


CLUSTER_HELPER = REPO_ROOT / "tests" / "integration" / "helpers" / "cluster.py"

OLD_RELEASE_IMAGE = "clickhouse/clickhouse-server"


def test_an_upgraded_old_release_instance_can_export(monkeypatch):
    """A compatibility suite starts an old release from `clickhouse/clickhouse-server`
    with `with_installed_binary=True` and then switches that same container to
    the binary under test (`restart_with_latest_version`). The image is not the
    integration-test one, so the export has to be decided by the binary the
    container can run, otherwise the whole upgraded phase is missing from the
    CI Logs cluster."""
    monkeypatch.setenv("DOCKER_BASE_TAG", "0-0-0")
    assert not HELPER.runs_binary_under_test(OLD_RELEASE_IMAGE, "24.3")
    assert HELPER.supports_export(OLD_RELEASE_IMAGE, "24.3", True)
    # An instance that stays on the old release never becomes exportable
    assert not HELPER.supports_export(OLD_RELEASE_IMAGE, "24.3", False)
    # And an instance of the image under test is exportable either way
    assert HELPER.supports_export(BASE_IMAGE, "0-0-0", False)
    assert HELPER.supports_export(BASE_IMAGE, "0-0-0", True)


def test_the_container_gets_the_credentials_before_the_export_is_enabled():
    """The `from_env` references of the cluster config are resolved when the
    server starts, and a `with_installed_binary` container writes that config
    only after the binary swap - so its `environment:` section has to be there
    from the start, i.e. keyed on `ci_logs_export_supported`."""
    source = CLUSTER_HELPER.read_text()
    guard = re.search(
        r"if (self\.ci_logs_export_\w+):\n"
        r"\s*ci_logs_env = ci_logs_export\.docker_compose_environment_section\(\)",
        source,
    )
    assert guard, f"no compose environment section in {CLUSTER_HELPER}"
    assert guard.group(1) == "self.ci_logs_export_supported"


def test_cache_directory_of_a_local_run_is_per_session(monkeypatch):
    """Without the CI identity the markers would be shared by every run on the
    machine, so a transient outage in one run would suppress the export in all
    the later ones."""
    for name in (
        "CLICKHOUSE_CI_LOGS_CACHE_DIR",
        HELPER.EXTRA_COLUMNS_EXPRESSION_HEAD_ENV,
        HELPER.EXTRA_COLUMNS_EXPRESSION_TAIL_ENV,
        "INTEGRATION_TESTS_RUN_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CLICKHOUSE_CI_LOGS_HOST", "logs.example.com")
    monkeypatch.setenv(HELPER.LOCAL_RUN_ID_ENV, "0" * 32)
    first = HELPER._cache_dir()
    monkeypatch.setenv(HELPER.LOCAL_RUN_ID_ENV, "1" * 32)
    assert HELPER._cache_dir() != first
    # The workers of one run share the id, and so the markers
    monkeypatch.setenv(HELPER.LOCAL_RUN_ID_ENV, "0" * 32)
    assert HELPER._cache_dir() == first


def test_a_successful_probe_wins_over_a_later_failure(tmp_path):
    """One pytest-xdist worker hitting a transient failure must not disable the
    export for the whole job after another worker has already connected."""
    assert HELPER._disabled_reason(tmp_path) is None
    (tmp_path / "disabled").write_text("cannot connect")
    assert HELPER._disabled_reason(tmp_path) == "cannot connect"
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "connected").touch()
    assert HELPER._disabled_reason(tmp_path) is None


def test_shutdown_flushes_the_async_insert_queue_before_the_senders():
    """The `_watcher` views insert through the `ci_logs_sender` profile, which
    has `async_insert = 1` and `wait_for_async_insert = 0`: the rows `SYSTEM
    FLUSH LOGS` materialises can still be in the asynchronous insert queue when
    the `_sender` tables are flushed, and are lost with the container unless
    the queue is flushed in between - the order the single-server exporter
    already uses."""
    statements = HELPER._shutdown_statements(["query_log", "text_log"])
    assert statements.index("SYSTEM FLUSH LOGS") < statements.index(
        "SYSTEM FLUSH ASYNC INSERT QUEUE"
    )
    senders = [s for s in statements if s.startswith("SYSTEM FLUSH DISTRIBUTED ")]
    assert senders == [
        "SYSTEM FLUSH DISTRIBUTED system.query_log_sender",
        "SYSTEM FLUSH DISTRIBUTED system.text_log_sender",
    ]
    assert all(
        statements.index("SYSTEM FLUSH ASYNC INSERT QUEUE") < statements.index(s)
        for s in senders
    )


def test_flush_before_shutdown_runs_the_statements_in_order():
    """`flush_before_shutdown` must send the whole sequence, in that order, as
    the queries of the instance; a server without export tables is left alone."""

    class Instance:
        name = "node"
        ci_logs_export_tables = ["query_log"]
        queries = []

        def query(self, sql, timeout=None):
            self.queries.append(sql)

    instance = Instance()
    HELPER.flush_before_shutdown(instance)
    assert len(instance.queries) == 1
    sent = [s for s in instance.queries[0].split(";\n") if s]
    assert sent == [
        "SYSTEM FLUSH LOGS",
        "SYSTEM FLUSH ASYNC INSERT QUEUE",
        "SYSTEM FLUSH DISTRIBUTED system.query_log_sender",
    ]

    class Idle:
        name = "idle"

        def query(self, sql, timeout=None):
            raise AssertionError("no export tables, nothing to flush")

    HELPER.flush_before_shutdown(Idle())


def _remote_tables_harness(monkeypatch, tmp_path, answers):
    """Run `_ensure_remote_tables` for one table against a fake CI Logs cluster
    whose answers to the successive remote queries are `answers`: a string is
    returned as the query output, an exception is raised."""
    monkeypatch.setattr(HELPER, "_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(HELPER.time, "sleep", lambda seconds: None)
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "connected").touch()
    queries = []
    answers = list(answers)

    def run(client_bin_path, sql, timeout=90, extra_args=()):
        queries.append(sql)
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(HELPER, "_run_remote_query", run)
    tables = [
        (
            "query_log",
            "abcd",
            "CREATE TABLE IF NOT EXISTS query_log_abcd (x UInt8) ORDER BY x",
        )
    ]
    created = HELPER._ensure_remote_tables("clickhouse", tables)
    markers = sorted(p.name for p in tmp_path.iterdir() if p.name != "connected")
    return created, markers, queries


def test_a_transient_failure_of_the_destination_ddl_is_retried(monkeypatch, tmp_path):
    """A connection reset while creating a destination table is retried on the
    schedule of the probe, and a success makes the table exportable; the
    transient failure must not be remembered as a failure of the table."""
    reset = RuntimeError("Code: 210. DB::NetException: Connection reset by peer")
    created, markers, queries = _remote_tables_harness(
        monkeypatch, tmp_path, [reset, reset, ""]
    )
    assert created == {"query_log"}
    assert markers == ["ok_query_log_abcd"]
    assert len(queries) == 3


def test_a_destination_table_is_only_blacklisted_once_confirmed_absent(
    monkeypatch, tmp_path
):
    """After the DDL failed, the table is checked on the cluster: it may well
    have been created while the client saw the error. Only a confirmed absence
    is remembered as `failed_...`, and if the check itself fails nothing is
    remembered, so the next server retries."""
    failure = RuntimeError("Code: 159. DB::Exception: Timeout exceeded")
    # The DDL did apply, the client only lost the answer
    created, markers, queries = _remote_tables_harness(
        monkeypatch, tmp_path, [failure, "1\n"]
    )
    assert created == {"query_log"}
    assert markers == ["ok_query_log_abcd"]
    assert queries[-1].startswith("EXISTS TABLE default.query_log_abcd")
    # The table really is absent
    created, markers, _ = _remote_tables_harness(
        monkeypatch, tmp_path / "absent", [failure, "0\n"]
    )
    assert created == set()
    assert markers == ["failed_query_log_abcd"]
    # The existence check fails too: nothing is remembered
    created, markers, _ = _remote_tables_harness(
        monkeypatch, tmp_path / "unknown", [failure, failure]
    )
    assert created == set()
    assert markers == []


def test_a_graceful_stop_flushes_the_export_first():
    """`stop_clickhouse` (and `restart_clickhouse`, which delegates to it) is
    how most suites restart a server: without a flush, the rows still in the
    log buffers, the asynchronous insert queue and the `_sender` queues are lost
    with the process. A hard kill is a crash simulation and must stay one."""
    source = CLUSTER_HELPER.read_text()
    start = source.index("    def stop_clickhouse(")
    body = source[start : source.index("\n    def ", start + 1)]
    flush = body.index("ci_logs_export.flush_before_shutdown(self)")
    assert body[:flush].rstrip().endswith("if not kill:")
    assert flush < body.index('"pkill {} clickhouse"')


def test_a_test_can_opt_a_server_out_of_the_export():
    """A server on a memory diet (`max_server_memory_usage` around 1 GB, which
    an ASan build fills at startup already) or one whose idle CPU time the test
    measures cannot afford the export; `add_instance(with_ci_logs_export=False)`
    must keep it off from the start, i.e. gate `ci_logs_export_supported`."""
    source = CLUSTER_HELPER.read_text()
    start = source.index("self.ci_logs_export_supported = (")
    expression = source[start : source.index("\n        )\n", start)]
    assert "and with_ci_logs_export" in expression
    assert "with_ci_logs_export=True," in source


def test_an_unrelated_watcher_is_not_taken_for_an_exported_table():
    """The helper keeps its `_sender`/`_watcher` tables in `system`, a namespace
    it shares with the test: the list of exported tables must only name the
    views the helper attempted to create, otherwise `flush_before_shutdown` and
    `teardown_for_instance` would flush and drop a test's own
    `system.<name>_watcher` as if the export owned it."""
    query = HELPER._active_tables_query(["query_log", "text_log"])
    assert "name IN ('query_log_watcher', 'text_log_watcher')" in query

    class Instance:
        name = "node"

        def query(self, sql, timeout=None):
            # The DDL batch ends with the scoped query, never an unscoped one
            assert "name IN ('query_log_watcher', 'text_log_watcher')" in sql
            return "query_log_watcher\ntext_log_watcher\n"

    tables = [("query_log", "h1", ""), ("text_log", "h2", ""), ("trace_log", "h3", "")]
    active = HELPER._create_senders_and_watchers(
        Instance(), tables, {"query_log", "text_log"}, "1 AS c"
    )
    assert active == ["query_log", "text_log"]
