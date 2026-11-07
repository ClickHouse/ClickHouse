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

import glob
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
# Keep in sync with `LogCluster.extra_columns_ddl` (ci/jobs/scripts/log_cluster.py),
# the source of `EXTRA_COLUMNS` for the functional tests: the same columns give
# the same structure hash, so functional and integration tests share the
# destination tables. This module runs inside the test runner container, where
# the `ci` package is not importable, so the definition is repeated here and
# their identity is checked by ci/tests/test_ci_logs_export_images.py.
EXTRA_COLUMNS = (
    "repo LowCardinality(String), pull_request_number UInt32, commit_sha String, "
    "check_start_time DateTime('UTC'), check_name LowCardinality(String), "
    "test_name LowCardinality(String), node_name LowCardinality(String), "
    "instance_type LowCardinality(String), instance_id String, "
    "workflow_start_time DateTime('UTC'), "
    "INDEX ix_repo (repo) TYPE set(100), INDEX ix_pr (pull_request_number) TYPE set(100), "
    "INDEX ix_commit (commit_sha) TYPE set(100), INDEX ix_check_time (check_start_time) TYPE minmax, "
    "INDEX ix_test (test_name) TYPE set(100), "
)
EXTRA_ORDER_BY_COLUMNS = "check_name, test_name"

# The values for the extra columns are provided by the CI job in two parts, in
# the EXTRA_COLUMNS order: everything before `test_name` and everything after
# `node_name` (the two per-server columns the helper fills in itself). The
# expression has to produce the columns in exactly the EXTRA_COLUMNS order:
# the local `*_sender` table is created as `SELECT {expression}, *`, so its
# header follows the expression, while the destination table follows
# EXTRA_COLUMNS. A different order is not a hard error - `Distributed` converts
# the block by name - but it logs a structure-mismatch warning for every
# exported batch, and since `system.text_log` is exported too, the export would
# feed its own warnings back into the stream.
EXTRA_COLUMNS_EXPRESSION_HEAD_ENV = "EXTRA_COLUMNS_EXPRESSION_HEAD"
EXTRA_COLUMNS_EXPRESSION_TAIL_ENV = "EXTRA_COLUMNS_EXPRESSION_TAIL"

# Used when the CI job did not provide the values (e.g. a manual local run with
# the credentials exported by hand).
DEFAULT_EXTRA_COLUMNS_EXPRESSION_HEAD = (
    "toLowCardinality('') AS repo, CAST(0 AS UInt32) AS pull_request_number, '' AS commit_sha, "
    "now('UTC') AS check_start_time, toLowCardinality('') AS check_name"
)
DEFAULT_EXTRA_COLUMNS_EXPRESSION_TAIL = (
    "toLowCardinality('') AS instance_type, '' AS instance_id, "
    "now('UTC') AS workflow_start_time"
)

# Set by tests/integration/conftest.py for a run without a `--run-id`, see
# _cache_dir.
LOCAL_RUN_ID_ENV = "INTEGRATION_TESTS_LOCAL_RUN_ID"

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



def _sender_profile_settings():
    """The settings of the `ci_logs_sender` profile of SENDER_USER_CONFIG, as
    `{name: value}`, without its constraints. The file is parsed by hand: it is
    a flat mapping, and PyYAML is not installed in the integration tests
    runner."""
    settings = {}
    in_profiles = False
    in_profile = False
    for line in Path(SENDER_USER_CONFIG).read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0:
            in_profiles = stripped == "profiles:"
            in_profile = False
        elif in_profiles and indent == 4:
            in_profile = stripped == f"{SENDER_USER}:"
        elif in_profile and indent == 8:
            name, _, value = stripped.partition(":")
            value = value.strip()
            if value:
                settings[name] = value
    return settings


# The settings of the `INSERT ... SELECT` that sends the rows the log tables
# already hold when the export is set up. It runs as the user of the test
# rather than through a `_watcher` view, so the `DEFINER = ci_logs_sender`
# profile does not apply to it, and the Distributed table stores the settings of
# the insert in its queue and replays them on the CI Logs cluster. It gets the
# same settings as the watchers, and no time limit, since the test may set a
# tiny `max_execution_time` in the `default` profile (e.g. 0.5 seconds in
# `test_backup_restore_new`).
BACKFILL_SETTINGS = {**_sender_profile_settings(), "max_execution_time": "0"}


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
    destination host.

    A manual local run provides none of them, so it is namespaced by the
    checkout and by the id of the pytest session instead (see
    tests/integration/conftest.py): the id is shared by the pytest-xdist workers
    of one run and differs between runs, so the markers of a run with a
    transient outage do not suppress the export in the next run.
    """
    explicit = os.environ.get("CLICKHOUSE_CI_LOGS_CACHE_DIR")
    if explicit:
        return Path(explicit)
    job_identity = [
        os.environ.get(name, "")
        for name in (
            EXTRA_COLUMNS_EXPRESSION_HEAD_ENV,
            EXTRA_COLUMNS_EXPRESSION_TAIL_ENV,
            "INTEGRATION_TESTS_RUN_ID",
            "CLICKHOUSE_CI_LOGS_HOST",
        )
    ]
    if not any(job_identity[:-1]):
        job_identity.append(str(Path(__file__).resolve().parents[3]))
        job_identity.append(os.environ.get(LOCAL_RUN_ID_ENV, ""))
    job_key = hashlib.sha1("\n".join(job_identity).encode()).hexdigest()[:16]
    return Path(tempfile.gettempdir()) / "clickhouse_ci_logs_export" / job_key


def _disabled_reason(cache_dir):
    """The reason the export was disabled for this job, or None.

    A successful probe is authoritative: once any pytest-xdist worker has proven
    the CI Logs cluster reachable, a transient failure in another worker must not
    disable the export for the rest of the job. The markers are only ever
    created, never removed, so the check does not race with a concurrent
    worker.

    The `lost` marker overrides it: it is written when the CI Logs cluster
    stops answering after the export is already running (see
    _disable_after_transport_failure), so that one outage in the middle of the
    job does not stall every later server stop for the flush timeout."""
    lost = cache_dir / "lost"
    if lost.exists():
        return lost.read_text()
    if (cache_dir / "connected").exists():
        return None
    marker = cache_dir / "disabled"
    if not marker.exists():
        return None
    return marker.read_text()


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


def remove_instance_config(config_d_dir, users_d_dir):
    """Undo write_instance_config and write_instance_users_config, for a server
    that is about to be restarted into an old release: it may not know the
    settings of the `ci_logs_sender` profile, and an unknown setting in a
    profile stops the server from starting."""
    for directory, name in (
        (config_d_dir, CLUSTER_CONFIG_NAME),
        (users_d_dir, USERS_CONFIG_NAME),
    ):
        path = os.path.join(directory, name)
        if os.path.exists(path):
            os.remove(path)


def runs_binary_under_test(image, tag):
    """Whether the containers of this image and tag run the ClickHouse binary
    built for the commit under test, see CURRENT_BINARY_IMAGE_TAG_ENV."""
    variable = CURRENT_BINARY_IMAGE_TAG_ENV.get(image)
    if variable is None:
        return False
    return tag == os.environ.get(variable, "latest")


def supports_export(image, tag, with_installed_binary):
    """Whether the container of an instance can carry the export at all - not
    whether it carries it right now (see `ClickHouseInstance.ci_logs_export_enabled`).

    A `with_installed_binary` instance starts from an old release installed over
    its image, whatever that image is, but the binary under test is mounted into
    it as `/usr/share/clickhouse_fresh` and `restart_with_latest_version` swaps
    the server to it. So the image decides only for the instances that never
    swap: the compatibility suites run the binary under test in the upgraded
    phase and export it, the old-release phases are excluded by
    `ci_logs_export_enabled` instead."""
    return with_installed_binary or runs_binary_under_test(image, tag)


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
    head = os.environ.get(
        EXTRA_COLUMNS_EXPRESSION_HEAD_ENV, DEFAULT_EXTRA_COLUMNS_EXPRESSION_HEAD
    )
    tail = os.environ.get(
        EXTRA_COLUMNS_EXPRESSION_TAIL_ENV, DEFAULT_EXTRA_COLUMNS_EXPRESSION_TAIL
    )
    return (
        f"{head}, toLowCardinality('{_escape_sql_string(test_name)}') AS test_name, "
        f"toLowCardinality('{_escape_sql_string(node_name)}') AS node_name, {tail}"
    )


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


def _is_transient_error(error):
    return any(marker in str(error) for marker in TRANSIENT_PROBE_ERRORS)


def _run_remote_query_with_retries(client_bin_path, sql, timeout=90, extra_args=()):
    """Run a query on the CI Logs cluster, retrying it up to PROBE_ATTEMPTS
    times when it fails with one of the transient errors. Raises the error of
    the last attempt."""
    for attempt in range(1, PROBE_ATTEMPTS + 1):
        try:
            return _run_remote_query(client_bin_path, sql, timeout=timeout, extra_args=extra_args)
        except Exception as e:
            error = e
        if attempt == PROBE_ATTEMPTS or not _is_transient_error(error):
            raise error
        logging.info(
            "CI logs export: attempt %s/%s of a query on the CI Logs cluster failed with a transient error, retrying",
            attempt,
            PROBE_ATTEMPTS,
        )
        time.sleep(attempt + 1)


def _probe_connection(client_bin_path):
    """Check that the CI Logs cluster is reachable. Returns None on success and
    the error of the last attempt otherwise."""
    try:
        _run_remote_query_with_retries(
            client_bin_path,
            "SELECT 1 FORMAT Null",
            timeout=60,
            extra_args=("--connect_timeout", "3"),
        )
    except Exception as e:
        return e
    return None


def _destination_table_exists(client_bin_path, table, hash_value):
    """Whether the destination table of `table` exists on the CI Logs cluster.
    Raises when that cannot be determined."""
    output = _run_remote_query_with_retries(
        client_bin_path,
        f"EXISTS TABLE default.{table}_{hash_value}",
        timeout=60,
        extra_args=("--connect_timeout", "3"),
    )
    return output.strip() == "1"


# The errors of a query on the CI Logs cluster that mean the cluster does not
# answer at all, as opposed to a failure of the query itself: the client gave up
# on the connection, or the client process did not finish in time.
REMOTE_UNAVAILABLE_ERRORS = (
    "ALL_CONNECTION_TRIES_FAILED",
    "SOCKET_TIMEOUT",
    "NETWORK_ERROR",
)


def _is_unavailable_error(error):
    if isinstance(error, subprocess.TimeoutExpired):
        return True
    return any(marker in str(error) for marker in REMOTE_UNAVAILABLE_ERRORS)


def _create_destination_table(client_bin_path, table, hash_value, statement):
    """Create the destination table of `table` on the CI Logs cluster. Returns
    True if it exists afterwards, False if it is known not to, and None if that
    could not be determined - the caller must not remember the latter. Raises
    when the existence check shows that the CI Logs cluster does not answer at
    all (see _is_unavailable_error): every other table would wait for the same
    timeouts.

    The DDL is idempotent (`CREATE TABLE IF NOT EXISTS`), so a transient failure
    is retried, and a failure of any other kind is followed by an existence
    check: the DDL may well have been applied on the cluster while the client
    saw a timeout or a reset connection, and a marker written on that evidence
    alone would suppress the export of the table for the rest of the job."""
    try:
        _run_remote_query_with_retries(client_bin_path, statement)
        return True
    except Exception:
        logging.warning(
            "CI logs export: failed to create the destination table for %s:\n%s",
            table,
            statement,
            exc_info=True,
        )
    try:
        return _destination_table_exists(client_bin_path, table, hash_value)
    except Exception as e:
        if _is_unavailable_error(e):
            raise
        logging.warning(
            "CI logs export: cannot tell whether the destination table for %s exists, will retry on the next server",
            table,
            exc_info=True,
        )
        return None


def _ensure_remote_tables(client_bin_path, tables):
    """Create the destination tables on the CI Logs cluster.

    The set of destination tables is the same for all servers of the job (the
    hash depends only on the table structure), so the result of each creation
    is cached on disk and shared between all tests and pytest-xdist workers of
    the job: only the first test that needs a table pays for the WAN round
    trip. Returns the set of tables which exist on the CI Logs cluster. A table
    is only remembered as failed once it is confirmed absent from the cluster
    after the DDL failed; a failure that leaves that unknown is retried by the
    next server.

    A failure to connect at all disables the export for the rest of the job
    (the marker file is checked on every call), so that a broken or unreachable
    CI Logs cluster does not slow down every test."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    connected_marker = cache_dir / "connected"
    if not connected_marker.exists():
        reason = _disabled_reason(cache_dir)
        if reason is not None:
            logging.info("CI logs export: disabled earlier in this job: %s", reason)
            return set()
        error = _probe_connection(client_bin_path)
        if error is not None:
            # Another worker may have proven the cluster reachable while this
            # probe was failing; its success wins over this transient failure.
            if not connected_marker.exists():
                reason = f"cannot connect to the CI Logs cluster: {error}"
                logging.warning("CI logs export: %s", reason)
                (cache_dir / "disabled").write_text(reason)
                return set()
        else:
            connected_marker.touch()

    created = set()
    lost_marker = cache_dir / "lost"
    for table, hash_value, statement in tables:
        ok_marker = cache_dir / f"ok_{table}_{hash_value}"
        failed_marker = cache_dir / f"failed_{table}_{hash_value}"
        if ok_marker.exists():
            created.add(table)
            continue
        if failed_marker.exists():
            continue
        # Another worker may have found the cluster gone in the meantime
        if lost_marker.exists():
            logging.info("CI logs export: disabled earlier in this job: %s", lost_marker.read_text())
            return set()
        try:
            exists = _create_destination_table(client_bin_path, table, hash_value, statement)
        except Exception as e:
            # The cluster answered the probe but stopped answering since. Stop
            # here, and disable the export for the rest of the job (the `lost`
            # marker overrides `connected`, see _disabled_reason): otherwise
            # every remaining table, and every later server, would wait for the
            # DDL and the existence check timeouts again.
            reason = f"the CI Logs cluster stopped answering while creating the destination table for {table}: {e}"
            logging.warning("CI logs export: disabled for the rest of the job: %s", reason)
            lost_marker.write_text(reason)
            return set()
        if exists is None:
            continue
        if not exists:
            logging.warning(
                "CI logs export: the destination table for %s does not exist on the CI Logs cluster, it will not be retried in this job",
                table,
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
    so that the exported logs identify the module that produced them.

    A cluster created in the `conftest.py` of a suite is shared by all modules
    of the suite, and the label is baked into the watcher materialized views
    once, when the server starts, so it cannot name the module of each row:
    such a cluster is labelled with the suite name, the narrowest unit that is
    still correct for all of its rows."""
    suite = os.path.basename(cluster.base_dir)
    module = os.path.splitext(os.path.basename(cluster.base_path))[0]
    if module in ("conftest", "__init__"):
        return suite
    # The modules pytest collects from the suite, see `python_files` in pytest.ini
    modules = glob.glob(os.path.join(cluster.base_dir, "test*.py"))
    if len(modules) <= 1:
        return suite
    return f"{suite}/{module}"


def setup_for_instance(cluster, instance, only_since_start=False):
    """Create the sender tables and the watcher materialized views on a started
    instance. Best effort: never raises.

    `only_since_start` limits the rows sent for what the log tables already
    hold to those written since the server started. It is for the first enable
    in `restart_with_latest_version`: the tables also hold the rows of the old
    release that ran before the swap, which must stay out of the export, see
    `supports_export`. The instance keeps that boundary, so that the tables
    whose setup fails here and is retried by `refresh_after_start` exclude the
    same rows, see `_upgrade_backfill_filters`."""
    if only_since_start:
        instance.ci_logs_export_since_upgrade = True
        instance.ci_logs_export_upgrade_time = None
    else:
        instance.ci_logs_export_since_upgrade = False
    try:
        _setup_for_instance(cluster, instance, only_since_start)
    except Exception:
        logging.warning(
            "CI logs export: failed to set up the export for instance %s",
            instance.name,
            exc_info=True,
        )


def _setup_for_instance(cluster, instance, only_since_start):
    reason = _disabled_reason(_cache_dir())
    if reason is not None:
        logging.info("CI logs export: disabled earlier in this job: %s", reason)
        return

    test_name = _test_name(cluster)
    expression = _extra_columns_expression(test_name, instance.name)

    # Every `instance.query` starts its own `clickhouse-client` process, which
    # costs about half a second regardless of the work the statement does. The
    # setup runs for every server of every test, so it is two round trips
    # (discovery, then all the DDL at once) rather than one round trip per log
    # table: with the ~17 log tables of a default instance, the latter added
    # ~11 s per server, which is most of an integration test job's wall clock.
    tables = _discover_log_tables(instance)

    exportable = _ensure_remote_tables(cluster.client_bin_path, tables)
    if not exportable:
        return

    backfill_filters = _upgrade_backfill_filters(
        instance, [table for table, _, _ in tables if table in exportable]
    )

    active = _create_senders_and_watchers(
        instance, tables, exportable, expression, backfill_filters
    )

    instance.ci_logs_export_tables = active
    instance.ci_logs_export_hashes = {
        table: hash_value for table, hash_value, _ in tables if table in active
    }
    logging.info(
        "CI logs export: enabled on %s/%s for tables: %s",
        test_name,
        instance.name,
        ", ".join(active),
    )


def refresh_after_start(cluster, instance):
    """Rewire the export of a server that has just been started again, for the
    log tables whose structure changed across the restart (e.g. a test switches
    `system.metric_log` to another schema in the config and restarts the
    server): the existing `_sender` table and `_watcher` view still have the
    header of the old structure and send to the destination table of the old
    hash. The other tables keep exporting through what was set up before.

    It also retries the tables that are not exported yet, including all of
    them when the setup at the first start ended with nothing exported (e.g.
    the existence of every destination table was unknown after a transient
    error of the CI Logs cluster, or the local DDL failed): a table without
    export state counts as changed, so a restart redoes the full setup instead
    of leaving the logs of the server unexported for the rest of the suite.
    Best effort: never raises."""
    try:
        _refresh_after_start(cluster, instance)
    except Exception:
        logging.warning(
            "CI logs export: failed to refresh the export for instance %s",
            instance.name,
            exc_info=True,
        )


def _refresh_after_start(cluster, instance):
    reason = _disabled_reason(_cache_dir())
    if reason is not None:
        logging.info("CI logs export: disabled earlier in this job: %s", reason)
        return

    # The discovery also does `SYSTEM FLUSH LOGS`, which is what makes a log
    # table with a changed structure rename the old table away and create the
    # new one, so the hashes below are those of the tables the server writes to.
    tables = _discover_log_tables(instance)
    known = getattr(instance, "ci_logs_export_hashes", {})
    changed = [entry for entry in tables if known.get(entry[0]) != entry[1]]
    if not changed:
        return

    stale = [table for table, _, _ in changed if table in known]
    if stale:
        # Send out what the stale `_sender` tables still hold, it has the old
        # structure and goes to the destination table of the old hash. The
        # flush of the discovery ran the stale `_watcher` views, and their rows
        # sit in the asynchronous insert queue until it is flushed, as in
        # `_shutdown_statements`.
        #
        # The objects are looked up first rather than taken from the export
        # state: a previous refresh that failed in the middle of this batch
        # dropped some of them while the state still names their tables as
        # stale, and a flush of a missing `_sender` would fail the whole batch
        # on every later restart, leaving those tables unexported for good.
        existing = set(instance.query(_active_tables_query(stale), timeout=60).split())
        senders = [f"system.{table}_sender" for table in stale if f"{table}_sender" in existing]
        statements = ""
        if senders:
            statements += f"SYSTEM FLUSH ASYNC INSERT QUEUE {', '.join(senders)};\n"
            statements += "".join(f"SYSTEM FLUSH DISTRIBUTED {sender};\n" for sender in senders)
        statements += "".join(
            f"DROP VIEW IF EXISTS system.{table}_watcher SYNC;\n"
            f"DROP TABLE IF EXISTS system.{table}_sender SYNC;\n"
            for table in stale
            if f"{table}_watcher" in existing or f"{table}_sender" in existing
        )
        if statements:
            instance.query(statements, timeout=300)
    exported = getattr(instance, "ci_logs_export_tables", [])
    active = [table for table in exported if table not in stale]
    hashes = {table: known[table] for table in active}
    instance.ci_logs_export_tables = active
    instance.ci_logs_export_hashes = hashes

    exportable = _ensure_remote_tables(cluster.client_bin_path, changed)
    if not exportable:
        return

    expression = _extra_columns_expression(_test_name(cluster), instance.name)
    backfill_filters = _upgrade_backfill_filters(
        instance, [table for table, _, _ in changed if table in exportable]
    )
    created = _create_senders_and_watchers(
        instance, changed, exportable, expression, backfill_filters
    )
    for table, hash_value, _ in changed:
        if table in created:
            hashes[table] = hash_value
    instance.ci_logs_export_tables = sorted(set(active) | set(created))
    logging.info(
        "CI logs export: re-created the export on %s for the changed tables: %s",
        instance.name,
        ", ".join(created),
    )


def _discover_log_tables(instance):
    """The system log tables of an instance: their names, the structure hashes
    of their destination tables and the statements that create those. The log
    tables are materialized first: most of them are created lazily on the first
    flush, and a table that does not exist yet has neither a structure to hash
    nor anything for a materialized view to attach to."""
    output = instance.query(
        "SYSTEM FLUSH LOGS;\n" + LOG_TABLES_QUERY,
        # formatQuery throws on statements longer than max_query_size, and the
        # CREATE statement of a wide log table (e.g. system.metric_log with
        # per-column comments) exceeds the default of 256 KiB
        settings={"param_extra_columns": EXTRA_COLUMNS, "max_query_size": "33554432"},
        timeout=120,
    )
    tables = []
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
    return tables


def _active_tables_query(tables):
    """The names of the `_watcher` views and the `_sender` tables of `tables`
    that exist on the instance; a table whose `_watcher` exists is one whose
    export is actually running. It is appended to the DDL batch instead of
    assuming that every statement of the batch succeeded, so that a partial
    failure gives a correct list rather than one that makes the shutdown flush
    fail on a `_sender` table which was never created. It is scoped to the
    tables this helper attempted to create, so that an unrelated
    `system.<name>_watcher` is never flushed or dropped as if it were ours."""
    names = ", ".join(
        f"'{_escape_sql_string(table + suffix)}'"
        for table in tables
        for suffix in ("_watcher", "_sender")
    )
    return f"SELECT name FROM system.tables WHERE database = 'system' AND name IN ({names})"


def _upgrade_backfill_filters(instance, tables):
    """The `backfill_filters` of `_create_senders_and_watchers` for an instance:
    `None` (send everything) unless the export was enabled by the switch of a
    `with_installed_binary` instance to the binary under test, see
    `setup_for_instance`. Then the boundary is the start of that binary, taken
    once, at the first setup after the switch, and kept on the instance: a
    later restart resets `uptime`, and the rows between the switch and that
    restart belong to the export. If the first setup failed before taking it,
    the start of the current server is used instead, which loses those rows
    rather than sending the rows of the old release."""
    if not getattr(instance, "ci_logs_export_since_upgrade", False):
        return None
    if getattr(instance, "ci_logs_export_upgrade_time", None) is None:
        instance.ci_logs_export_upgrade_time = _server_start_time(instance)
    return _backfill_filters_since(instance, tables, instance.ci_logs_export_upgrade_time)


def _server_start_time(instance):
    """The Unix time the server of the instance started at."""
    return int(instance.query("SELECT toUnixTimestamp(now()) - uptime()", timeout=60).strip())


def _backfill_filters_since(instance, tables, start_time):
    """The conditions that keep, of the rows a log table already holds, only
    those written since `start_time`: `{table: condition}` for the tables
    with an `event_time` column. A table without it is left out, and its rows
    from before the `_watcher` existed are not sent at all, since nothing tells
    the rows of the server apart from those of the binary that ran before it."""
    if not tables:
        return {}
    names = ", ".join(f"'{_escape_sql_string(table)}'" for table in tables)
    output = instance.query(
        "SELECT table FROM system.columns "
        f"WHERE database = 'system' AND name = 'event_time' AND table IN ({names})",
        timeout=60,
    )
    return {
        table: f"event_time >= toDateTime({int(start_time)})"
        for table in output.split()
    }


def _create_senders_and_watchers(instance, tables, exportable, expression, backfill_filters=None):
    """`backfill_filters` restricts the rows already in the log tables that are
    sent: `None` sends all of them, otherwise only the rows matching the
    condition of the table, and nothing for a table without one, see
    `_backfill_filters_since_start`."""
    cluster_name = _cluster_name()
    backfill_settings = ", ".join(f"{name} = {value}" for name, value in BACKFILL_SETTINGS.items())
    statements = []
    for table, hash_value, _ in tables:
        if table not in exportable:
            continue
        if backfill_filters is None:
            backfill = (
                f"INSERT INTO system.{table}_sender SETTINGS {backfill_settings} "
                f"SELECT {expression}, * FROM system.{table};"
            )
        elif table in backfill_filters:
            backfill = (
                f"INSERT INTO system.{table}_sender SETTINGS {backfill_settings} "
                f"SELECT {expression}, * FROM system.{table} WHERE {backfill_filters[table]};"
            )
        else:
            backfill = ""
        # The callers pass only the tables without export state, so whatever
        # exists for them is left over from an earlier attempt that failed
        # (e.g. a `_sender` whose cleanup below failed too). Dropping it in the
        # same batch, before `CREATE TABLE IF NOT EXISTS`, is what keeps such a
        # leftover from being reused with the destination table of an older
        # structure: if the drop fails, the batch stops before the `CREATE`, and
        # the table stays unexported until the next attempt drops it again.
        #
        # The discovery has just flushed the logs into the table, and a
        # materialized view only sees the blocks inserted after it is created,
        # so the rows already in the table - the startup of the server, which is
        # what matters most when it fails to boot - are sent explicitly. That
        # happens before the `_watcher` is created, so that no block reaches the
        # `_sender` twice; a flush that lands between the two statements is
        # lost, which is a much shorter window than the flush interval.
        statements.append(
            f"""
            DROP VIEW IF EXISTS system.{table}_watcher SYNC;
            DROP TABLE IF EXISTS system.{table}_sender SYNC;

            CREATE TABLE IF NOT EXISTS system.{table}_sender
            ENGINE = Distributed({cluster_name}, 'default', '{table}_{hash_value}')
            SETTINGS flush_on_detach = 0
            EMPTY AS SELECT {expression}, * FROM system.{table};

            {backfill}

            CREATE MATERIALIZED VIEW IF NOT EXISTS system.{table}_watcher
            TO system.{table}_sender
            DEFINER = {SENDER_USER}
            AS SELECT {expression}, * FROM system.{table};
            """
        )
    if not statements:
        return []
    active_tables_query = _active_tables_query(
        table for table, _, _ in tables if table in exportable
    )
    try:
        output = instance.query("".join(statements) + active_tables_query, timeout=300)
    except Exception:
        logging.warning(
            "CI logs export: failed to create the sender/watcher tables on %s",
            instance.name,
            exc_info=True,
        )
        # The statements before the failing one did take effect
        output = instance.query(active_tables_query, timeout=60)
    names = output.split()
    active = sorted(name[: -len("_watcher")] for name in names if name.endswith("_watcher"))
    # A batch that failed between the `CREATE TABLE` of a `_sender` and the
    # `CREATE MATERIALIZED VIEW` of its `_watcher` leaves the `_sender` behind.
    # Nothing records it, so it is dropped right away. This cleanup may fail
    # without harm: the `_sender` receives nothing without its `_watcher`, and
    # the next attempt for the table drops it before creating a new one, see
    # the statements above.
    orphans = sorted(
        name[: -len("_sender")]
        for name in names
        if name.endswith("_sender") and name[: -len("_sender")] not in active
    )
    if orphans:
        try:
            instance.query(
                "".join(f"DROP TABLE IF EXISTS system.{table}_sender SYNC;\n" for table in orphans),
                timeout=120,
            )
        except Exception:
            logging.warning(
                "CI logs export: failed to drop the sender tables left without a watcher on %s: %s",
                instance.name,
                ", ".join(orphans),
                exc_info=True,
            )
    return active


def _shutdown_statements(tables):
    """The flushes that push the tail of the logs out of a server about to stop,
    in the order the data travels: the log rows are materialised from the buffers
    of the system tables, the `_watcher` views run as `ci_logs_sender` whose
    profile has `async_insert = 1` with `wait_for_async_insert = 0`, so the rows
    they produce sit in the asynchronous insert queue until it is flushed, and
    only then do they reach the `_sender` tables the last statements send out.
    Mirrors `stop` in `ci/jobs/scripts/log_export.py`, except that the
    asynchronous insert queue is flushed only for the `_sender` tables."""
    senders = [f"system.{table}_sender" for table in tables]
    return [
        "SYSTEM FLUSH LOGS",
        # Only the queues of the `_sender` tables: a flush of the whole queue
        # also waits for the batches of the tables of the test, which may be
        # held on purpose (e.g. by a failpoint) while the test stops the server.
        f"SYSTEM FLUSH ASYNC INSERT QUEUE {', '.join(senders)}",
        *(f"SYSTEM FLUSH DISTRIBUTED {sender}" for sender in senders),
    ]


def teardown_for_instance(instance):
    """Flush what has been collected so far and remove the export from a running
    server, for an instance that is about to be restarted into an old release:
    the `_watcher` views would keep referencing a `DEFINER` that
    remove_instance_config takes away.

    The flush is best effort, but a failure to drop the tables propagates: the
    views left behind would reference the `DEFINER` that is about to be removed,
    so the caller must not proceed to remove the configs. The export state of
    the instance is cleared only after the drops succeed, so that it still
    names the tables to clean up if they did not."""
    tables = getattr(instance, "ci_logs_export_tables", None)
    if not tables:
        return
    flush_before_shutdown(instance)
    statements = "".join(
        f"DROP VIEW IF EXISTS system.{table}_watcher SYNC;\n"
        f"DROP TABLE IF EXISTS system.{table}_sender SYNC;\n"
        for table in tables
    )
    instance.query(statements, timeout=120)
    instance.ci_logs_export_tables = []
    instance.ci_logs_export_hashes = {}


# The errors of a flush that mean the CI Logs cluster does not answer, as
# opposed to a failure of the local server: the client of the test runner timed
# out, or a `SYSTEM FLUSH DISTRIBUTED` failed to reach the cluster.
TRANSPORT_FAILURE_ERRORS = (
    "Client timed out",
    "ALL_CONNECTION_TRIES_FAILED",
    "SOCKET_TIMEOUT",
    "NETWORK_ERROR",
    "TIMEOUT_EXCEEDED",
)


def _disable_after_transport_failure(instance, error):
    """Disable the export for the rest of the job when a flush of a running
    export failed because the CI Logs cluster does not answer. Every later
    flush would wait for the same timeout, and integration tests stop and
    restart many servers, so a single outage would otherwise add the timeout to
    each of them. A failure is attributed to the CI Logs cluster only when the
    local server still answers: a server that died or is being killed by the
    test fails the flush too, but says nothing about the export."""
    if not any(marker in str(error) for marker in TRANSPORT_FAILURE_ERRORS):
        return
    try:
        instance.query("SELECT 1", timeout=10)
    except Exception:
        return
    reason = f"the CI Logs cluster stopped answering, a flush on {instance.name} failed: {error}"
    logging.warning("CI logs export: disabled for the rest of the job: %s", reason)
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "lost").write_text(reason)


def flush_before_shutdown(instance):
    """Flush the logs, the asynchronous insert queue and the pending Distributed
    sends, so that the data accumulated since the last flush is not lost with
    the container. Skipped once the export was disabled for the job, see
    _disable_after_transport_failure. Best effort: never raises."""
    tables = getattr(instance, "ci_logs_export_tables", None)
    if not tables:
        return
    reason = _disabled_reason(_cache_dir())
    if reason is not None:
        logging.info(
            "CI logs export: not flushing the logs of %s, disabled earlier in this job: %s",
            instance.name,
            reason,
        )
        return
    try:
        statements = "".join(f"{statement};\n" for statement in _shutdown_statements(tables))
        instance.query(statements, timeout=120)
    except Exception as e:
        logging.warning(
            "CI logs export: failed to flush the logs on shutdown of %s",
            instance.name,
            exc_info=True,
        )
        _disable_after_transport_failure(instance, e)
