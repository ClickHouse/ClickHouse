"""Post-hoc export of the system log tables from the performance comparison
servers (left = reference, right = patched) to the CI Logs cluster.

Functional and integration tests forward log blocks to the CI Logs cluster
continuously, through materialized views created next to each system log table
(see ci/jobs/scripts/functional_tests/setup_log_cluster.sh). The performance
comparison must not do any extra work while queries are being measured, so
instead the export runs once, after all tests have finished and before the
servers are stopped:

  - for every non-empty system.*_log table a destination table
    `<table>_<hash>` is created on the CI Logs cluster (the hash covers the
    table structure, so servers of different versions export into different
    tables - the left server runs an older build and its log tables may
    differ);
  - the accumulated data is pushed by the server itself with a single
    `INSERT INTO FUNCTION remoteSecure(...) SELECT <extra columns>, *` query.

The extra columns are exactly the same as in the other checks, so that the
structure hash - and therefore the destination table - is shared with them.
There is no dedicated column for the server, so the two servers are told apart
by a suffix in `check_name`: '<job name> (left)' is the reference build and
'<job name> (right)' is the patched one. `commit_sha` carries each server's own
build commit as well; that lookup is the one fail-closed step here (see
get_server_commit_sha), because a row whose build cannot be named is of no use
in the CI Logs cluster.

The export is best effort otherwise: any failure is logged and never fails the
job.

The credentials are taken from the CLICKHOUSE_CI_LOGS_HOST / _USER / _PASSWORD
environment variables (see setup_credentials_env); they are passed to
clickhouse-client via stdin and the CLICKHOUSE_PASSWORD environment variable,
so they never appear in the process list, and the remoteSecure arguments are
masked as '[HIDDEN]' in the server logs. Any error output is scrubbed before
logging, since the job log is a public CI artifact.
"""

import json
import os
import re
import subprocess
import time

# Extra columns added to every exported table. Keep in sync with EXTRA_COLUMNS
# in ci/jobs/scripts/functional_tests/setup_log_cluster.sh: the same columns
# give the same structure hash, so all checks share the destination tables.
EXTRA_COLUMNS = (
    "repo LowCardinality(String), pull_request_number UInt32, commit_sha String, "
    "check_start_time DateTime('UTC'), check_name LowCardinality(String), "
    "instance_type LowCardinality(String), instance_id String, "
    "INDEX ix_repo (repo) TYPE set(100), INDEX ix_pr (pull_request_number) TYPE set(100), "
    "INDEX ix_commit (commit_sha) TYPE set(100), INDEX ix_check_time (check_start_time) TYPE minmax, "
)
EXTRA_ORDER_BY_COLUMNS = "check_name"

# Returns one row per system log table: its name, the structure hash of the
# destination table, the multi-line CREATE statement and the number of rows.
# The hash expression is equivalent to the one in setup_log_cluster.sh (an
# array of N copies of the extra columns definition, and an array of
# (name, type) ordered by position), and formatQuery output is identical to
# SHOW CREATE TABLE output.
LOG_TABLES_QUERY = """
SELECT
    t.name AS table,
    toString(c.h) AS hash,
    formatQuery(t.create_table_query) AS statement,
    toString(coalesce(t.total_rows, 1)) AS total_rows
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

# Destination tables already created (or failed) in this process: both servers
# usually share most table structures, so do the remote DDL only once.
_created_remote_tables = set()
_failed_remote_tables = set()


def setup_credentials_env():
    """Make sure the CI Logs cluster credentials are present in the
    environment, fetching them from AWS SSM if needed. Returns False (with a
    warning printed) if the export cannot be done."""
    if os.environ.get("CLICKHOUSE_CI_LOGS_HOST"):
        return True
    try:
        from ci.praktika import Secret

        host, password = (
            Secret.Config(
                name="clickhouse_ci_logs_host",
                type=Secret.Type.AWS_SSM_PARAMETER,
                region="us-east-1",
            )
            .join_with(
                Secret.Config(
                    name="clickhouse_ci_logs_password",
                    type=Secret.Type.AWS_SSM_PARAMETER,
                    region="us-east-1",
                )
            )
            .get_value()
        )
    except Exception as e:
        print(f"WARNING: Failed to fetch the CI Logs cluster credentials, the logs will not be exported: {e}")
        return False
    if not host or not password:
        print("WARNING: Empty CI Logs cluster credentials, the logs will not be exported")
        return False
    os.environ["CLICKHOUSE_CI_LOGS_HOST"] = host
    os.environ["CLICKHOUSE_CI_LOGS_USER"] = "ci"
    os.environ["CLICKHOUSE_CI_LOGS_PASSWORD"] = password
    return True


def _scrub(text):
    """Remove the credential values from any output that goes to the job log."""
    for var in ("CLICKHOUSE_CI_LOGS_PASSWORD", "CLICKHOUSE_CI_LOGS_HOST"):
        value = os.environ.get(var, "")
        # Very short values would mangle unrelated parts of the output
        if len(value) >= 4:
            text = text.replace(value, f"[{var}]")
    return text


def _run_client(args, sql, timeout, env=None):
    """Run clickhouse-client with the query on stdin (so that credentials
    inside the query never appear in the process list)."""
    try:
        result = subprocess.run(
            ["clickhouse-client"] + args,
            input=sql,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=timeout,
        )
    except subprocess.SubprocessError as e:
        # `TimeoutExpired` and the other subprocess exceptions stringify the
        # full argv, which contains the real CI Logs host. Re-raise a scrubbed
        # error, and suppress the exception chaining, so that the original
        # unscrubbed message cannot reach the public job log through a
        # traceback either.
        raise RuntimeError(f"clickhouse-client failed: {_scrub(str(e))}") from None
    if result.returncode != 0:
        raise RuntimeError(f"clickhouse-client failed with code {result.returncode}: {_scrub(result.stderr.strip())}")
    return result.stdout


def _server_query(port, sql, timeout, extra_args=None):
    return _run_client(["--port", str(port)] + (extra_args or []), sql, timeout)


# `system.build_options` reports the full git hash of the build; it is empty
# only if the binary was built without git information available.
GIT_HASH_RE = re.compile(r"[0-9a-f]{7,40}")


def get_server_commit_sha(port):
    """Return the git hash of the build of the server listening on the given
    local port.

    Unlike everything else in this module this is not best effort: `commit_sha`
    is a part of the identity of every exported row, so a server whose build
    cannot be named must be skipped by the caller instead of contributing rows
    that cannot be attributed to a build. `_server_query` raises if the query
    fails, and an answer that is not a git hash is rejected here."""
    sha = _server_query(
        port,
        "SELECT value FROM system.build_options WHERE name = 'GIT_HASH'",
        timeout=60,
    ).strip()
    if not GIT_HASH_RE.fullmatch(sha):
        raise RuntimeError(f"the server reported an unusable build commit: '{sha}'")
    return sha


def _remote_query(sql, timeout=90, extra_args=None):
    """Run a query on the CI Logs cluster. The password is passed through the
    environment, see Client.cpp handling of CLICKHOUSE_PASSWORD."""
    args = [
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
        "--distributed_ddl_output_mode",
        "throw_only_active",
        # The CREATE statement of a wide log table (e.g. system.metric_log with
        # per-column comments) exceeds the default max_query_size of 256 KiB
        "--max_query_size",
        "33554432",
    ]
    if os.environ.get("CLICKHOUSE_CI_LOGS_SECURE", "1") != "0":
        args.append("--secure")
    env = os.environ.copy()
    env["CLICKHOUSE_PASSWORD"] = os.environ.get("CLICKHOUSE_CI_LOGS_PASSWORD", "")
    return _run_client(args + (extra_args or []), sql, timeout, env=env)


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


def _escape_sql_string(value):
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def _extra_columns_expression(repo, pr_number, commit_sha, check_start_time, check_name, node_name, instance_type, instance_id):
    # NOTE: the expressions must go in the same order as EXTRA_COLUMNS: the
    # INSERT SELECT into the remote table function maps columns by position.
    # There is no separate column for the server, so it is encoded in
    # `check_name`, see the module docstring.
    check_name = f"{check_name} ({node_name})"
    return (
        f"toLowCardinality('{_escape_sql_string(repo)}') AS repo, "
        f"CAST({int(pr_number)} AS UInt32) AS pull_request_number, "
        f"'{_escape_sql_string(commit_sha)}' AS commit_sha, "
        f"toDateTime('{_escape_sql_string(check_start_time)}', 'UTC') AS check_start_time, "
        f"toLowCardinality('{_escape_sql_string(check_name)}') AS check_name, "
        f"toLowCardinality('{_escape_sql_string(instance_type)}') AS instance_type, "
        f"'{_escape_sql_string(instance_id)}' AS instance_id"
    )


# The remote CI Logs cluster occasionally fails a probe for reasons that are
# transient and unrelated to our credentials: it resets the connection, or it is
# momentarily over its memory limit and rejects the query with
# `MEMORY_LIMIT_EXCEEDED` (its RSS crosses the cap for a few seconds under load
# from other CI jobs). Only these cases are worth retrying; anything else
# (authentication, configuration, DNS, a real outage) will not recover, and the
# probe must not slow the job down. Keep in sync with
# `check_logs_credentials` in
# ci/jobs/scripts/functional_tests/setup_log_cluster.sh.
TRANSIENT_PROBE_ERRORS = ("Connection reset by peer", "MEMORY_LIMIT_EXCEEDED")
PROBE_ATTEMPTS = 3


def _remote_available():
    """Probe the CI Logs cluster once per process, so that an unreachable
    cluster costs one connection timeout instead of one per table."""
    global _remote_available_cache
    if _remote_available_cache is not None:
        return _remote_available_cache
    for attempt in range(1, PROBE_ATTEMPTS + 1):
        try:
            # A short connect timeout (instead of the 10s default) is applied to
            # the probe only, so that a real outage fails fast. The export
            # queries keep the default timeouts.
            _remote_query("SELECT 1 FORMAT Null", timeout=60, extra_args=["--connect_timeout", "3"])
            _remote_available_cache = True
            return True
        except Exception as e:
            error = str(e)
            if attempt == PROBE_ATTEMPTS or not any(x in error for x in TRANSIENT_PROBE_ERRORS):
                print(f"WARNING: Cannot connect to the CI Logs cluster, the logs will not be exported: {error}")
                _remote_available_cache = False
                return False
            print(f"Attempt {attempt}/{PROBE_ATTEMPTS} to connect to the CI Logs cluster failed (transient error), retrying: {error}")
            time.sleep(attempt + 1)
    return False


_remote_available_cache = None


def _ensure_remote_table(table, hash_value, statement):
    """Create the destination table on the CI Logs cluster (once per process).
    Returns True if the table exists there."""
    key = f"{table}_{hash_value}"
    if key in _created_remote_tables:
        return True
    if key in _failed_remote_tables:
        return False
    try:
        _remote_query(statement)
    except Exception as e:
        print(f"WARNING: Failed to create the destination table {key} on the CI Logs cluster: {e}")
        _failed_remote_tables.add(key)
        return False
    _created_remote_tables.add(key)
    return True


def export_system_logs_from_server(
    port,
    node_name,
    repo,
    pr_number,
    commit_sha,
    check_start_time,
    check_name,
    instance_type,
    instance_id,
    insert_timeout=600,
):
    """Export all non-empty system.*_log tables from the server listening on
    the given local port to the CI Logs cluster. Best effort: failures are
    printed and swallowed."""
    try:
        _export_system_logs_from_server(
            port,
            node_name,
            repo,
            pr_number,
            commit_sha,
            check_start_time,
            check_name,
            instance_type,
            instance_id,
            insert_timeout,
        )
    except Exception as e:
        print(f"WARNING: Failed to export system logs from the [{node_name}] server: {e}")


def _export_system_logs_from_server(
    port,
    node_name,
    repo,
    pr_number,
    commit_sha,
    check_start_time,
    check_name,
    instance_type,
    instance_id,
    insert_timeout,
):
    expression = _extra_columns_expression(
        repo,
        pr_number,
        commit_sha,
        check_start_time,
        check_name,
        node_name,
        instance_type,
        instance_id,
    )

    if not _remote_available():
        return

    _server_query(port, "SYSTEM FLUSH LOGS", timeout=300)

    tables = []
    output = _server_query(
        port,
        LOG_TABLES_QUERY,
        timeout=120,
        # formatQuery throws on statements longer than max_query_size, and the
        # CREATE statement of a wide log table (e.g. system.metric_log with
        # per-column comments) exceeds the default of 256 KiB
        extra_args=[
            "--param_extra_columns",
            EXTRA_COLUMNS,
            "--max_query_size",
            "33554432",
        ],
    )
    for line in output.splitlines():
        if line.strip():
            tables.append(json.loads(line))

    remote_host = os.environ["CLICKHOUSE_CI_LOGS_HOST"]
    remote_port = os.environ.get("CLICKHOUSE_CI_LOGS_PORT", "9440")
    remote_user = os.environ.get("CLICKHOUSE_CI_LOGS_USER", "ci")
    remote_password = os.environ.get("CLICKHOUSE_CI_LOGS_PASSWORD", "")
    remote_fn = "remoteSecure" if os.environ.get("CLICKHOUSE_CI_LOGS_SECURE", "1") != "0" else "remote"

    exported = []
    for row in tables:
        table = row["table"]
        if row["total_rows"] == "0":
            print(f"Table system.{table} on the [{node_name}] server is empty, skipping")
            continue
        statement = _adapt_create_statement(table, row["hash"], row["statement"])
        if not _ensure_remote_table(table, row["hash"], statement):
            continue
        # The INSERT runs on the perf server itself; the password inside the
        # query text is masked as '[HIDDEN]' in query_log and the server log.
        insert = (
            f"INSERT INTO FUNCTION {remote_fn}('{remote_host}:{remote_port}', 'default', "
            f"'{table}_{row['hash']}', '{remote_user}', '{_escape_sql_string(remote_password)}')\n"
            f"SELECT {expression}, * FROM system.{table}"
        )
        started = time.time()
        try:
            _server_query(
                port,
                insert,
                timeout=insert_timeout + 60,
                extra_args=[
                    "--max_execution_time",
                    str(insert_timeout),
                    "--max_memory_usage",
                    "10G",
                    "--max_threads",
                    "4",
                    "--max_rows_to_read",
                    "0",
                    "--max_bytes_to_read",
                    "0",
                    "--max_result_rows",
                    "0",
                    "--max_result_bytes",
                    "0",
                    "--network_compression_method",
                    "zstd",
                ],
            )
        except Exception as e:
            print(f"WARNING: Failed to export system.{table} ({row['total_rows']} rows) from the [{node_name}] server: {e}")
            continue
        exported.append(table)
        print(f"Exported system.{table} ({row['total_rows']} rows) from the [{node_name}] server in {time.time() - started:.1f}s")

    print(f"Export of system logs from the [{node_name}] server is done, exported tables: {', '.join(exported) if exported else 'none'}")
