"""Export of system log tables (system.query_log, system.text_log, ...) from the
ClickHouse servers started by integration tests to the CI Logs cluster.

This is the integration-tests counterpart of
ci/jobs/scripts/functional_tests/setup_log_cluster.sh, which does the same for
functional tests, stress tests and fuzzers. For every eligible server instance,
after the cluster is started:

  - a remote destination table `<table>_<hash>` is created on the CI Logs
    cluster for every `system.*_log` table (the hash covers the structure, so
    servers of different versions can export into the same cluster);
  - a Distributed table `system.<table>_sender` pointing to the destination
    table and a materialized view `system.<table>_watcher` are created locally,
    so every flushed log block is forwarded to the CI Logs cluster.

The views are created with `DEFINER = ci_logs_sender`, a dedicated user with a
pinned profile (the same tests/config/users.d/ci_logs_sender.yaml the functional
tests install, see SENDER_USER_CONFIG), so the export runs with its own short
timeouts and async-insert settings instead of the settings of the test query
that happened to trigger the log flush.

The destination tables are augmented with extra columns describing the CI job
(repo, commit, check name, ...) plus two integration-test specific columns:
`test_name` (the test module, e.g. `test_storage_s3` or
`test_prometheus_protocols/test_series_api`) and `node_name` (the name
of the instance within the cluster, e.g. `node1`).

The export is enabled by the CLICKHOUSE_CI_LOGS_HOST environment variable (set
by ci/jobs/integration_test_job.py in CI). It is best effort: any failure is
logged and never fails the tests themselves.

Security note: the credentials must never be materialized in files that can be
collected as CI artifacts (everything under the instance directories is).
The remote cluster config therefore references them with `from_env` (and hides
them in the preprocessed config), and the values are passed into the containers
through the docker compose process environment.
"""

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

# Extra columns added to every exported table, and the values for them.
# Keep in sync with EXTRA_COLUMNS in
# ci/jobs/scripts/functional_tests/setup_log_cluster.sh: the same columns give
# the same structure hash, so functional and integration tests share the
# destination tables.
EXTRA_COLUMNS = (
    "repo LowCardinality(String), pull_request_number UInt32, commit_sha String, "
    "check_start_time DateTime('UTC'), check_name LowCardinality(String), "
    "test_name LowCardinality(String), node_name LowCardinality(String), "
    "instance_type LowCardinality(String), instance_id String, "
    "INDEX ix_repo (repo) TYPE set(100), INDEX ix_pr (pull_request_number) TYPE set(100), "
    "INDEX ix_commit (commit_sha) TYPE set(100), INDEX ix_check_time (check_start_time) TYPE minmax, "
    "INDEX ix_test (test_name) TYPE set(100), "
)
EXTRA_ORDER_BY_COLUMNS = "check_name, test_name"

# Used when the CI job did not provide the values (e.g. a manual local run with
# the credentials exported by hand).
DEFAULT_EXTRA_COLUMNS_EXPRESSION = (
    "toLowCardinality('') AS repo, CAST(0 AS UInt32) AS pull_request_number, '' AS commit_sha, "
    "now() AS check_start_time, toLowCardinality('') AS check_name, "
    "toLowCardinality('') AS instance_type, '' AS instance_id"
)

# The names are chosen to sort after the test-provided configs, so that the
# definitions survive test configs which merge the same sections with
# `replace="replace"` (the config.d/users.d files are merged in the
# alphabetical order).
CLUSTER_CONFIG_NAME = "zzz_system_logs_export.xml"
USERS_CONFIG_NAME = "zzz_ci_logs_sender.yaml"

# The user the `_watcher` materialized views run as, and the config that defines
# it together with its profile. That config is the one the functional tests,
# `clickbench`, `sqlstorm` and the fuzzers install (tests/config/install.sh and
# ci/jobs/scripts/fuzzer/run-fuzzer.sh copy the same file), and it is installed
# here rather than copied into this module so that the pinned settings and their
# readonly constraints cannot drift between the two export paths.
SENDER_USER = "ci_logs_sender"
SENDER_USER_CONFIG = os.path.join(
    os.path.dirname(__file__), "..", "..", "config", "users.d", "ci_logs_sender.yaml"
)

# The images whose instances run the binary under test: the base integration
# test image, and the images derived from it (`FROM clickhouse/integration-test`
# in ci/docker/integration/*/Dockerfile), each with the environment variable
# that carries its tag. An instance of any other image
# (`clickhouse/clickhouse-server` at an old release tag,
# `clickhouse/python-bottle` for a mock HTTP service) runs something else and
# may not support the configs and the DDL the export needs.
# Keep in sync with `IMAGES_ENV` in
# ci/jobs/scripts/integration_tests_configs.py; both the map and the derived
# images are checked by ci/tests/test_ci_logs_export_images.py.
CURRENT_BINARY_IMAGE_TAG_ENV = {
    "clickhouse/integration-test": "DOCKER_BASE_TAG",
    "clickhouse/integration-test-with-unity-catalog": "DOCKER_BASE_WITH_UNITY_CATALOG_TAG",
}

CLUSTER_CONFIG_TEMPLATE = """<clickhouse>
    <remote_servers>
        <{cluster}>
            <shard>
                <replica>
                    <secure>{secure}</secure>
                    <host from_env="CLICKHOUSE_CI_LOGS_HOST" hide_in_preprocessed="1"/>
                    <port>{port}</port>
                    <user from_env="CLICKHOUSE_CI_LOGS_USER" hide_in_preprocessed="1"/>
                    <password from_env="CLICKHOUSE_CI_LOGS_PASSWORD" hide_in_preprocessed="1"/>
                </replica>
            </shard>
        </{cluster}>
    </remote_servers>
</clickhouse>
"""

# Returns one row per system log table: its name, the structure hash of the
# destination table, and the multi-line CREATE statement. The hash expression
# is equivalent to the one in setup_log_cluster.sh (an array of N copies of the
# extra columns definition, and an array of (name, type) ordered by position),
# and formatQuery output is identical to SHOW CREATE TABLE output.
LOG_TABLES_QUERY = """
SELECT
    t.name AS table,
    toString(c.h) AS hash,
    formatQuery(t.create_table_query) AS statement
FROM system.tables AS t
INNER JOIN
(
    SELECT
        table,
        sipHash64(
            arrayResize(CAST([] AS Array(String)), toUInt64(count()), {extra_columns:String}),
            arrayMap(x -> (tupleElement(x, 2), tupleElement(x, 3)),
                arraySort(x -> tupleElement(x, 1), groupArray((position, name, type))))
        ) AS h
    FROM system.columns
    WHERE database = 'system' AND endsWith(table, '_log')
    GROUP BY table
) AS c ON c.table = t.name
WHERE t.database = 'system' AND endsWith(t.name, '_log') AND t.engine LIKE '%MergeTree'
ORDER BY table
FORMAT JSONEachRow
"""


def is_enabled():
    return bool(os.environ.get("CLICKHOUSE_CI_LOGS_HOST"))


def _cluster_name():
    name = os.environ.get("CLICKHOUSE_CI_LOGS_CLUSTER", "system_logs_export")
    assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name), name
    return name


def _cache_dir():
    """The directory with the marker files shared between all the tests and all
    the pytest-xdist workers of one CI job.

    The markers must not outlive the job that created them: a transient outage
    in one run must not suppress the export in a later run that happens to land
    on the same (reused) worker. The default location is therefore namespaced by
    the identity of the job: the values for the extra columns (they contain the
    check name, the commit and the check start time), the run id and the
    destination host. A run without them (a manual local run) gets its own
    namespace as well, shared for the whole checkout.
    """
    explicit = os.environ.get("CLICKHOUSE_CI_LOGS_CACHE_DIR")
    if explicit:
        return Path(explicit)
    job_identity = "\n".join(
        os.environ.get(name, "")
        for name in (
            "EXTRA_COLUMNS_EXPRESSION",
            "INTEGRATION_TESTS_RUN_ID",
            "CLICKHOUSE_CI_LOGS_HOST",
        )
    )
    job_key = hashlib.sha1(job_identity.encode()).hexdigest()[:16]
    return Path(tempfile.gettempdir()) / "clickhouse_ci_logs_export" / job_key


def write_instance_config(config_d_dir):
    """Add the CI Logs cluster to the instance config. The credentials are not
    written to the file: they are taken from the container environment, see
    docker_compose_environment_section."""
    # The variables referenced with from_env must exist in the container even
    # if empty, otherwise the server refuses to start.
    os.environ.setdefault("CLICKHOUSE_CI_LOGS_USER", "ci")
    os.environ.setdefault("CLICKHOUSE_CI_LOGS_PASSWORD", "")
    config = CLUSTER_CONFIG_TEMPLATE.format(
        cluster=_cluster_name(),
        secure=1 if os.environ.get("CLICKHOUSE_CI_LOGS_SECURE", "1") != "0" else 0,
        port=int(os.environ.get("CLICKHOUSE_CI_LOGS_PORT", "9440")),
    )
    with open(os.path.join(config_d_dir, CLUSTER_CONFIG_NAME), "w") as f:
        f.write(config)


def write_instance_users_config(users_d_dir):
    """Add the `ci_logs_sender` profile and user to the instance config, see
    SENDER_USER_CONFIG."""
    shutil.copyfile(SENDER_USER_CONFIG, os.path.join(users_d_dir, USERS_CONFIG_NAME))


def runs_binary_under_test(image, tag):
    """Whether the containers of this image and tag run the ClickHouse binary
    built for the commit under test, see CURRENT_BINARY_IMAGE_TAG_ENV."""
    variable = CURRENT_BINARY_IMAGE_TAG_ENV.get(image)
    if variable is None:
        return False
    return tag == os.environ.get(variable, "latest")


def without_sender_user(query_result):
    """Drop the lines that mention the `ci_logs_sender` user or its profile from
    the result of an access-control introspection query (`SHOW USERS`,
    `SHOW CREATE USERS`, `SHOW GRANTS FOR ALL`, `SHOW PROFILES`,
    `SHOW ACCESS`, ...). The export installs them on every instance when it is
    enabled and on none when it is not, so a test that asserts the complete
    access-control state of an instance has to ignore them."""
    return "\n".join(
        line for line in query_result.split("\n") if SENDER_USER not in line
    )


def docker_compose_environment_section():
    """An `environment:` section for the instance service in the docker compose
    file. The variables are listed without values: docker compose takes them
    from its own process environment, so the credentials never appear in the
    compose file (which is collected as a CI artifact)."""
    return "environment:\n            - CLICKHOUSE_CI_LOGS_HOST\n            - CLICKHOUSE_CI_LOGS_USER\n            - CLICKHOUSE_CI_LOGS_PASSWORD"


def _escape_sql_string(value):
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _extra_columns_expression(test_name, node_name):
    base = os.environ.get("EXTRA_COLUMNS_EXPRESSION", DEFAULT_EXTRA_COLUMNS_EXPRESSION)
    return f"{base}, toLowCardinality('{_escape_sql_string(test_name)}') AS test_name, toLowCardinality('{_escape_sql_string(node_name)}') AS node_name"


def _adapt_create_statement(table, hash_value, statement):
    """Transform the local CREATE TABLE statement into the statement for the
    destination table on the CI Logs cluster. The transformations mirror
    setup_log_cluster.sh: add the extra columns, prepend the extra ORDER BY
    columns, rename the table, drop TTL/SETTINGS/COMMENT, strip per-column
    comments (wide tables like system.metric_log expand into a CREATE larger
    than the default max_query_size otherwise)."""
    result = []
    for line in statement.split("\n"):
        line = re.sub(r" COMMENT '([^'\\]|\\.)*'", "", line)
        if re.fullmatch(r"CREATE TABLE system\.\w+_log", line):
            line = f"CREATE TABLE IF NOT EXISTS {table}_{hash_value}"
        elif line == "(":
            line = "(" + EXTRA_COLUMNS
        elif line.startswith(("TTL ", "SETTINGS ", "COMMENT ")):
            continue
        else:
            match = re.fullmatch(r"ORDER BY (?:([^(].*)|\((.*)\))", line)
            if match:
                order_by = match.group(1) or match.group(2)
                line = f"ORDER BY ({EXTRA_ORDER_BY_COLUMNS}, {order_by})"
        result.append(line)
    result.append("SETTINGS use_const_adaptive_granularity = 1")
    return "\n".join(result)


def _run_remote_query(client_bin_path, sql, timeout=90, extra_args=()):
    """Run a query on the CI Logs cluster. The password is passed through the
    environment so that it appears neither in logs nor in the process list."""
    command = [client_bin_path]
    if os.path.basename(client_bin_path) == "clickhouse":
        command.append("client")
    command += [
        "--host",
        os.environ["CLICKHOUSE_CI_LOGS_HOST"],
        "--port",
        os.environ.get("CLICKHOUSE_CI_LOGS_PORT", "9440"),
        "--user",
        os.environ.get("CLICKHOUSE_CI_LOGS_USER", "ci"),
        "--receive_timeout",
        "45",
        "--send_timeout",
        "45",
        # The destination database may be Replicated
        "--database_replicated_initial_query_timeout_sec",
        "10",
        "--distributed_ddl_task_timeout",
        "30",
        # Wide log tables produce large CREATE statements even with the
        # per-column comments stripped
        "--max_query_size",
        "33554432",
        "--distributed_ddl_output_mode",
        "throw_only_active",
    ]
    if os.environ.get("CLICKHOUSE_CI_LOGS_SECURE", "1") != "0":
        command.append("--secure")
    command += list(extra_args)
    env = os.environ.copy()
    env["CLICKHOUSE_PASSWORD"] = os.environ.get("CLICKHOUSE_CI_LOGS_PASSWORD", "")
    result = subprocess.run(
        command,
        input=sql,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Query on the CI Logs cluster failed with code {result.returncode}: {result.stderr}")
    return result.stdout


# The remote CI logs cluster occasionally fails a probe for reasons that are
# transient and unrelated to our credentials: it resets the connection, or it is
# momentarily over its memory limit and rejects the query. Only those cases are
# worth retrying; anything else (auth, DNS, a real outage) will not recover, and
# the probe must not slow down every test. Keep in sync with
# check_logs_credentials in setup_log_cluster.sh.
TRANSIENT_PROBE_ERRORS = ("Connection reset by peer", "MEMORY_LIMIT_EXCEEDED")
PROBE_ATTEMPTS = 3


def _probe_connection(client_bin_path):
    """Check that the CI Logs cluster is reachable. Returns None on success and
    the error of the last attempt otherwise."""
    for attempt in range(1, PROBE_ATTEMPTS + 1):
        try:
            _run_remote_query(
                client_bin_path,
                "SELECT 1 FORMAT Null",
                timeout=60,
                extra_args=("--connect_timeout", "3"),
            )
            return None
        except Exception as e:
            error = e
        if not any(marker in str(error) for marker in TRANSIENT_PROBE_ERRORS):
            break
        if attempt == PROBE_ATTEMPTS:
            break
        logging.info(
            "CI logs export: attempt %s/%s to connect to the CI Logs cluster failed with a transient error, retrying",
            attempt,
            PROBE_ATTEMPTS,
        )
        time.sleep(attempt + 1)
    return error


def _ensure_remote_tables(client_bin_path, tables):
    """Create the destination tables on the CI Logs cluster.

    The set of destination tables is the same for all servers of the job (the
    hash depends only on the table structure), so the result of each creation
    is cached on disk and shared between all tests and pytest-xdist workers of
    the job: only the first test that needs a table pays for the WAN round
    trip. Returns the set of tables which exist on the CI Logs cluster.

    A failure to connect at all disables the export for the rest of the job
    (the marker file is checked on every call), so that a broken or unreachable
    CI Logs cluster does not slow down every test."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    disabled_marker = cache_dir / "disabled"
    if disabled_marker.exists():
        logging.info(
            "CI logs export: disabled earlier in this job: %s",
            disabled_marker.read_text(),
        )
        return set()

    if not (cache_dir / "connected").exists():
        error = _probe_connection(client_bin_path)
        if error is not None:
            reason = f"cannot connect to the CI Logs cluster: {error}"
            logging.warning("CI logs export: %s", reason)
            disabled_marker.write_text(reason)
            return set()
        (cache_dir / "connected").touch()

    created = set()
    for table, hash_value, statement in tables:
        ok_marker = cache_dir / f"ok_{table}_{hash_value}"
        failed_marker = cache_dir / f"failed_{table}_{hash_value}"
        if ok_marker.exists():
            created.add(table)
            continue
        if failed_marker.exists():
            continue
        try:
            _run_remote_query(client_bin_path, statement)
        except Exception:
            logging.warning(
                "CI logs export: failed to create the destination table for %s (will not be retried in this job):\n%s",
                table,
                statement,
                exc_info=True,
            )
            failed_marker.touch()
            continue
        ok_marker.touch()
        created.add(table)
    return created


def _test_name(cluster):
    """The name of the pytest module the cluster belongs to: the name of the
    suite directory for a single-file suite (`test_storage_s3`), and the module
    inside it for a multi-file suite (`test_prometheus_protocols/test_series_api`),
    so that the exported logs identify the module that produced them."""
    suite = os.path.basename(cluster.base_dir)
    module = os.path.splitext(os.path.basename(cluster.base_path))[0]
    if module == "test":
        return suite
    return f"{suite}/{module}"


def setup_for_instance(cluster, instance):
    """Create the sender tables and the watcher materialized views on a started
    instance. Best effort: never raises."""
    try:
        _setup_for_instance(cluster, instance)
    except Exception:
        logging.warning(
            "CI logs export: failed to set up the export for instance %s",
            instance.name,
            exc_info=True,
        )


def _setup_for_instance(cluster, instance):
    disabled_marker = _cache_dir() / "disabled"
    if disabled_marker.exists():
        logging.info(
            "CI logs export: disabled earlier in this job: %s",
            disabled_marker.read_text(),
        )
        return

    test_name = _test_name(cluster)
    expression = _extra_columns_expression(test_name, instance.name)

    # Materialize all configured system log tables before reading their structure
    instance.query("SYSTEM FLUSH LOGS", timeout=120)

    tables = []
    output = instance.query(
        LOG_TABLES_QUERY,
        # formatQuery throws on statements longer than max_query_size, and the
        # CREATE statement of a wide log table (e.g. system.metric_log with
        # per-column comments) exceeds the default of 256 KiB
        settings={"param_extra_columns": EXTRA_COLUMNS, "max_query_size": "33554432"},
        timeout=120,
    )
    for line in output.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        tables.append(
            (
                row["table"],
                row["hash"],
                _adapt_create_statement(row["table"], row["hash"], row["statement"]),
            )
        )

    exportable = _ensure_remote_tables(cluster.client_bin_path, tables)
    if not exportable:
        return

    cluster_name = _cluster_name()
    active = []
    for table, hash_value, _ in tables:
        if table not in exportable:
            continue
        try:
            instance.query(
                f"""
                CREATE TABLE IF NOT EXISTS system.{table}_sender
                ENGINE = Distributed({cluster_name}, 'default', '{table}_{hash_value}')
                SETTINGS flush_on_detach = 0
                EMPTY AS SELECT {expression}, * FROM system.{table};

                CREATE MATERIALIZED VIEW IF NOT EXISTS system.{table}_watcher
                TO system.{table}_sender
                DEFINER = {SENDER_USER}
                AS SELECT {expression}, * FROM system.{table};
                """,
                timeout=60,
            )
        except Exception:
            logging.warning(
                "CI logs export: failed to create the sender/watcher for %s on %s",
                table,
                instance.name,
                exc_info=True,
            )
            continue
        active.append(table)

    instance.ci_logs_export_tables = active
    logging.info(
        "CI logs export: enabled on %s/%s for tables: %s",
        test_name,
        instance.name,
        ", ".join(active),
    )


def flush_before_shutdown(instance):
    """Flush the logs and the pending Distributed sends, so that the data
    accumulated since the last flush is not lost with the container.
    Best effort: never raises."""
    tables = getattr(instance, "ci_logs_export_tables", None)
    if not tables:
        return
    try:
        statements = "SYSTEM FLUSH LOGS;\n" + "\n".join(f"SYSTEM FLUSH DISTRIBUTED system.{table}_sender;" for table in tables)
        instance.query(statements, timeout=120)
    except Exception:
        logging.warning(
            "CI logs export: failed to flush the logs on shutdown of %s",
            instance.name,
            exc_info=True,
        )
