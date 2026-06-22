#!/usr/bin/env bash
# Test that allow_experimental_detach_queries in clickhouse-local (LocalConnection)
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
    echo "@@SKIP@@: script utility not available (needed for PTY)"
    exit 0
fi

# Run queries in clickhouse-local through a PTY (so is_interactive=true).
# `script -q -c CMD /dev/null` discards the typescript but gives CMD a real TTY as stdin/stdout.
# Strip ANSI escape sequences and carriage returns so output is stable.
# A trailing "\q" ensures clickhouse-local exits cleanly even after errors
# (ExceptionBeforeStart leaves the interactive prompt waiting for more input
# while the PTY from `script` stays open).  A short sleep separates it from
# the queries so the interactive parser does not consume it as part of a
# multi-line statement.
run_local_interactive() {
    { printf '%s\n' "$@"; sleep 1; echo '\q'; } \
        | script -q -c "${CLICKHOUSE_LOCAL}" /dev/null 2>/dev/null \
        | sed 's/\x1b\[[0-9;?]*[A-Za-z]//g; s/\r//g'
}

# --- 1. Successful detach: INSERT into existing table returns a query_id block ---
echo "=== Local: Detach mode: INSERT-SELECT returns query_id ==="
OUT=$(run_local_interactive \
    "CREATE TABLE t_local_detach (x UInt64) ENGINE=Memory;" \
    "INSERT INTO t_local_detach SELECT 99 SETTINGS allow_experimental_detach_queries=1, async_insert=0;")

# The detached INSERT returns a single-column "query_id" block; verify it appeared.
if echo "$OUT" | grep -q "query_id"; then
    echo "Detach returned query_id: yes"
else
    echo "Detach returned query_id: no"
    echo "FAIL: Expected 'query_id' in detached INSERT output, got: $OUT"
    exit 1
fi

# NOTE: we intentionally do not assert that a follow-up "SELECT * FROM t_local_detach" sees the
# inserted row. `sendQuery` no longer blocks on a still-running detached query (blocking would
# serialize every follow-up command behind it and defeat detached execution), so a follow-up
# command is not ordered after the detached INSERT. Completion is guaranteed only when the
# connection is destroyed (the destructor joins the background work). Background execution of the
# INSERT itself is covered deterministically by 03812 over HTTP/native.

# --- 2. ExceptionBeforeStart: INSERT into nonexistent table must return an error, not query_id ---
echo "=== Local: ExceptionBeforeStart — error returned when query fails before start ==="
OUT_ERR=$(run_local_interactive \
    "INSERT INTO table_does_not_exist_04053 SELECT 1 SETTINGS allow_experimental_detach_queries=1, async_insert=0;")

if echo "$OUT_ERR" | grep -qi "UNKNOWN_TABLE\|does not exist"; then
    echo "Error returned to client: yes"
else
    echo "Error returned to client: no"
    echo "FAIL: Expected UNKNOWN_TABLE error on INSERT into nonexistent table, got: $OUT_ERR"
    exit 1
fi

# --- 3. SELECT is also detached when the setting is on: returns query_id, not the row ---
echo "=== Local: SELECT is detached when allow_experimental_detach_queries=1 ==="
OUT_SEL=$(run_local_interactive \
    "SELECT 42 SETTINGS allow_experimental_detach_queries=1, async_insert=0;" \
    "SELECT 'sync_value';")

# Detached SELECT returns the single-column "query_id" block (the value "42" is discarded). The
# next query runs synchronously; it does not block on the detached SELECT.
if echo "$OUT_SEL" | grep -q "query_id"; then
    echo "SELECT returned query_id: yes"
else
    echo "SELECT returned query_id: no"
    echo "FAIL: Expected 'query_id' in detached SELECT output, got: $OUT_SEL"
    exit 1
fi

# The follow-up sync SELECT must still produce its result regardless of the detached SELECT.
if echo "$OUT_SEL" | grep -q "sync_value"; then
    echo "Follow-up sync SELECT returned: yes"
else
    echo "Follow-up sync SELECT returned: no"
    echo "FAIL: Expected sync SELECT result after detach, got: $OUT_SEL"
    exit 1
fi

echo "OK"
