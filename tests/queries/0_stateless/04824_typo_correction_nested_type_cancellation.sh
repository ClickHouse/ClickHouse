#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the deeply nested fixture takes seconds to create.

# Typo correction for an unresolved identifier walks every subcolumn of every candidate column.
# The substream tree doubles per nesting level, and the walk observed no time limit, so a query on a
# deeply nested type ignored `max_execution_time` while it was still being analyzed.
# https://github.com/ClickHouse/ClickHouse/pull/86768#issuecomment-5224028011

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The server echoes the exception, hint text included, as a log line at the default level, which
# would match the greps below a second time.
CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT/--send_logs_level=$CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL/--send_logs_level=none}

nested="UInt8"
for _ in $(seq 1 12); do
    nested="Array(Map(String, Tuple(a ${nested}, b ${nested})))"
done

# The type string is ~150 KB, so feed the DDL on stdin rather than as an argument, and leave the
# query size limit a few times above it.
echo "CREATE TABLE t_04824 (c ${nested}, plain UInt8) ENGINE = Memory" \
    | $CLICKHOUSE_CLIENT --max_query_size=1000000

# Only the analyzer collects hints this way, so every statement below that depends on the walk pins
# it: on an old-analyzer configuration the walk never runs and the assertions would be vacuous.

# A one-part identifier cannot match a compound subcolumn at any depth, so none of the four walks
# per column can contribute a hint. They ran anyway, taking 44 s on a debug build and 348 s under a
# sanitizer. The limit is far above what the query now costs, so this is not a timing race: it
# reports UNKNOWN_IDENTIFIER rather than TIMEOUT_EXCEEDED.
$CLICKHOUSE_CLIENT -q "SELECT nosuchcolumn FROM t_04824 SETTINGS enable_analyzer = 1, max_execution_time = 10" 2>&1 \
    | grep -c "UNKNOWN_IDENTIFIER"

# A two-part identifier keeps the one walk that can contribute a hint, which is still expensive on a
# type this deep, so that walk must observe the limit. Before the fix the query ran to completion and
# reported UNKNOWN_IDENTIFIER, ignoring the limit entirely.
$CLICKHOUSE_CLIENT -q "SELECT a.nosuchcolumn FROM t_04824 AS a SETTINGS enable_analyzer = 1, max_execution_time = 0.001" 2>&1 \
    | grep -c "TIMEOUT_EXCEEDED"

# The 'break' overflow mode never marks the query cancelled, so observing cancellation alone would
# leave the limit unenforced here.
$CLICKHOUSE_CLIENT -q "SELECT a.nosuchcolumn FROM t_04824 AS a SETTINGS enable_analyzer = 1, max_execution_time = 0.001, timeout_overflow_mode = 'break'" 2>&1 \
    | grep -c "TIMEOUT_EXCEEDED"

# clickhouse-local runs no deadline watchdog thread at all, for either overflow mode. The type string
# is past the per-argument length limit, so the script goes in a file rather than on the command line.
cat > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.sql" <<EOF
CREATE TABLE t_04824_local (c ${nested}) ENGINE = Memory;
SELECT a.nosuchcolumn FROM t_04824_local AS a SETTINGS enable_analyzer = 1, max_execution_time = 0.001;
EOF
$CLICKHOUSE_LOCAL --max_query_size=1000000 --queries-file "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.sql" 2>&1 \
    | grep -c "TIMEOUT_EXCEEDED"

# A cancelled query must still report its own cause rather than a timeout. The walk is short once the
# limit is observed, so retry until the kill lands: a query that reports TIMEOUT_EXCEEDED here, or
# never reports the cancellation, fails the test.
kill_verdict="kill never landed"
for _ in $(seq 1 30); do
    query_id="04824_kill_${CLICKHOUSE_DATABASE}_${RANDOM}"
    $CLICKHOUSE_CLIENT --query_id "$query_id" \
        -q "SELECT a.nosuchcolumn FROM t_04824 AS a SETTINGS enable_analyzer = 1, max_execution_time = 0" > /dev/null 2> "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.err" &
    client_pid=$!

    for _ in $(seq 1 500); do
        [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" = "1" ] && break
    done
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC" > /dev/null
    wait $client_pid

    if grep -q "TIMEOUT_EXCEEDED" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.err"; then
        kill_verdict="kill reported a timeout"
        break
    fi
    if grep -q "QUERY_WAS_CANCELLED" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.err"; then
        kill_verdict="kill reported cancellation"
        break
    fi
done
echo "$kill_verdict"

# The hints themselves must not change. This one can only come from the walk: an alias expression has
# no column list to draw suggestions from, unlike a table, whose column map already holds subcolumn
# names and so keeps producing them either way. Both candidates pass the edit distance filter, and
# 'aa' is strictly closer to 'ab' than 'zz' is, so the winner does not depend on iteration order.
$CLICKHOUSE_CLIENT -q "SELECT x.ab FROM (SELECT (1, 2)::Tuple(aa UInt8, zz UInt8) AS x) SETTINGS enable_analyzer = 1" 2>&1 \
    | grep -o "Maybe you meant: \['x.aa'\]"

# A one-part identifier must still be answered from the plain column names.
$CLICKHOUSE_CLIENT -q "SELECT plai FROM t_04824 SETTINGS enable_analyzer = 1" 2>&1 | grep -o "Maybe you meant: \['plain'\]"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04824"
rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.err" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.sql"
