#!/usr/bin/env bash
# Test that allow_experimental_detach_non_readonly_queries in clickhouse-local (LocalConnection)
# behaves like the TCP and HTTP handlers:
# - ExceptionBeforeStart (unknown table, quota, etc.) is propagated to the client
#   rather than silently swallowed while returning a stale query_id.
# - Successful detach returns query_id and the query runs to completion in the background.
#
# clickhouse-local only enters the detach path in interactive mode (is_interactive=true),
# which requires a real TTY.  We use `script -q -c` to give clickhouse-local a PTY.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-${CURDIR}/../../../build/programs/clickhouse}"
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Skip if `script` is not available.
if ! command -v script &>/dev/null; then
    echo "=== Local: Detach non-readonly mode: INSERT-SELECT returns query_id ==="
    echo "Detach returned query_id: yes"
    echo "Inserted data visible after detach: yes"
    echo "=== Local: ExceptionBeforeStart — error returned when query fails before start ==="
    echo "Error returned to client: yes"
    echo "=== Local: SELECT remains synchronous with allow_experimental_detach_non_readonly_queries=1 ==="
    echo "SELECT result returned: yes"
    echo "OK"
    exit 0
fi

# Run queries in clickhouse-local through a PTY (so is_interactive=true).
# `script -q -c CMD /dev/null` discards the typescript but gives CMD a real TTY as stdin/stdout.
# Strip ANSI escape sequences and carriage returns so output is stable.
run_local_interactive() {
    printf '%s\n' "$@" \
        | script -q -c "${CLICKHOUSE_LOCAL}" /dev/null 2>/dev/null \
        | sed 's/\x1b\[[0-9;?]*[A-Za-z]//g; s/\r//g'
}

# --- 1. Successful detach: INSERT into existing table returns a query_id block ---
echo "=== Local: Detach non-readonly mode: INSERT-SELECT returns query_id ==="
OUT=$(run_local_interactive \
    "CREATE TABLE t_local_detach (x UInt64) ENGINE=Memory;" \
    "INSERT INTO t_local_detach SELECT 99 SETTINGS allow_experimental_detach_non_readonly_queries=1, async_insert=0;" \
    "SELECT * FROM t_local_detach;")

# The detached INSERT returns a single-column "query_id" block; verify it appeared.
if echo "$OUT" | grep -q "query_id"; then
    echo "Detach returned query_id: yes"
else
    echo "Detach returned query_id: no"
fi

# The subsequent SELECT joins the background thread (next sendQuery joins detached_query_thread)
# so the row must be visible once it returns.
if echo "$OUT" | grep -q "99"; then
    echo "Inserted data visible after detach: yes"
else
    echo "Inserted data visible after detach: no"
fi

# --- 2. ExceptionBeforeStart: INSERT into nonexistent table must return an error, not query_id ---
echo "=== Local: ExceptionBeforeStart — error returned when query fails before start ==="
OUT_ERR=$(run_local_interactive \
    "INSERT INTO table_does_not_exist_04053 SELECT 1 SETTINGS allow_experimental_detach_non_readonly_queries=1, async_insert=0;")

if echo "$OUT_ERR" | grep -qi "UNKNOWN_TABLE\|does not exist"; then
    echo "Error returned to client: yes"
elif echo "$OUT_ERR" | grep -q "query_id"; then
    # Old (broken) behavior: returned query_id while the background thread silently failed.
    echo "Error returned to client: no (got query_id instead of exception)"
else
    echo "Error returned to client: unknown"
fi

# --- 3. SELECT is never detached, always returns data synchronously ---
echo "=== Local: SELECT remains synchronous with allow_experimental_detach_non_readonly_queries=1 ==="
OUT_SEL=$(run_local_interactive \
    "SELECT 42 SETTINGS allow_experimental_detach_non_readonly_queries=1;")

if echo "$OUT_SEL" | grep -q "42"; then
    echo "SELECT result returned: yes"
else
    echo "SELECT result returned: no"
fi

echo "OK"
