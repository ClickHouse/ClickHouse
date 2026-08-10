#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the kill arm below runs the walk with no time limit, so it costs the full tree.

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
for _ in $(seq 1 10); do
    nested="Array(Map(String, Tuple(a ${nested}, b ${nested})))"
done

# The kill arm has to interrupt the walk while it runs, and the walk is short once the limit is
# observed, so that arm needs a wider window than the assertions above it.
nested_wide="${nested}"
for _ in $(seq 1 4); do
    nested_wide="Array(Map(String, Tuple(a ${nested_wide}, b ${nested_wide})))"
done

# A cast in a subquery reaches the walk without storing a column of the type. Naming it in a DDL
# would walk the same exponential tree again in `ColumnsDescription`, which no time limit bounds.
deep="(SELECT CAST([], '${nested}') AS c) AS a"
deep_wide="(SELECT CAST([], '${nested_wide}') AS c) AS a"

# The type strings reach ~600 KB, so feed queries on stdin rather than as an argument.
run_query() {
    echo "SELECT a.nosuchcolumn FROM $1 SETTINGS enable_analyzer = 1, $2" \
        | $CLICKHOUSE_CLIENT --max_query_size=8000000 "${@:3}"
}

# Only the analyzer collects hints this way, so every statement below that depends on the walk pins
# it: on an old-analyzer configuration the walk never runs and the assertions would be vacuous.

# A two-part identifier keeps the one walk that can contribute a hint, which is still expensive on a
# type this deep, so that walk must observe the limit. Before the fix the query ran to completion and
# reported UNKNOWN_IDENTIFIER, ignoring the limit entirely.
run_query "$deep" "max_execution_time = 0.001" 2>&1 | grep -c "TIMEOUT_EXCEEDED"

# The 'break' overflow mode never marks the query cancelled, so observing cancellation alone would
# leave the limit unenforced here.
run_query "$deep" "max_execution_time = 0.001, timeout_overflow_mode = 'break'" 2>&1 | grep -c "TIMEOUT_EXCEEDED"

# clickhouse-local runs no deadline watchdog thread at all, for either overflow mode. The script goes
# in a file rather than on the command line to keep the long type string off the argument list.
cat > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.sql" <<EOF
SELECT a.nosuchcolumn FROM ${deep} SETTINGS enable_analyzer = 1, max_execution_time = 0.001;
EOF
$CLICKHOUSE_LOCAL --max_query_size=8000000 --queries-file "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.sql" 2>&1 \
    | grep -c "TIMEOUT_EXCEEDED"

# A cancelled query must still report its own cause rather than a timeout. The walk is short once the
# limit is observed, so retry until the kill lands: a query that reports TIMEOUT_EXCEEDED here, or
# never reports the cancellation, fails the test.
kill_verdict="kill never landed"
kill_deadline=$(($(date +%s) + 60))
while [ "$(date +%s)" -lt "$kill_deadline" ]; do
    query_id="04824_kill_${CLICKHOUSE_DATABASE}_${RANDOM}"
    run_query "$deep_wide" "max_execution_time = 0" --query_id "$query_id" \
        > /dev/null 2> "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.err" &
    client_pid=$!

    # Paced, because an unpaced poll competes with the walk it is waiting for.
    poll_deadline=$(($(date +%s) + 10))
    while [ "$(date +%s)" -lt "$poll_deadline" ]; do
        [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" = "1" ] && break
        sleep 0.1
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

# A one-part identifier must still be answered from the plain column names of a table.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04824 (c Array(Map(String, Tuple(a UInt8, b UInt8))), plain UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "SELECT plai FROM t_04824 SETTINGS enable_analyzer = 1" 2>&1 | grep -o "Maybe you meant: \['plain'\]"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04824"
rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.err" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.sql"
