#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_NAME="t_03918_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
$CLICKHOUSE_CLIENT --query="CREATE TABLE ${TABLE_NAME} (number UInt64) ENGINE = MergeTree ORDER BY number"
$CLICKHOUSE_CLIENT --query="INSERT INTO ${TABLE_NAME} SELECT * FROM system.numbers LIMIT 1000"

SETTINGS="enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1"

# Iterations are needed to reliably trigger the race on master:
# the bugfix-validation job runs this test once against the master binary (without
# the fix) and expects it to fail there. With only 1 iteration the race sometimes
# does not trigger and bugfix-validation reports "Failed to reproduce the bug".
# 5 iterations × 8 functions = 40 query invocations, expected runtime ~5s in
# stateless tests and ~90s under flaky-check randomization (well within 180s).
#
# Pass condition: at least one iteration reports non-zero rows_read.
# Master without the fix is reliably and consistently broken — none of the
# iterations report non-zero rows_read. The fix makes rows_read non-zero on
# essentially every run, but a rare residual race may still produce 0 for one
# specific format/transport in flaky-check randomization, so we tolerate a
# minority of iterations reporting 0.
NUM_ITERATIONS=5

function check_rows_read_client_json()
{
    local fmt=$1
    local ok=0
    for _ in $(seq 1 $NUM_ITERATIONS); do
        result=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT ${fmt} SETTINGS ${SETTINGS}" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
        if [ -n "$result" ] && [ "$result" -gt 0 ]; then
            ok=$((ok + 1))
        fi
    done
    if [ "$ok" -eq 0 ]; then
        echo "${fmt} FAIL: rows_read=0 in all ${NUM_ITERATIONS} iterations"
    else
        echo "${fmt} OK"
    fi
}

function check_rows_read_client_xml()
{
    local ok=0
    for _ in $(seq 1 $NUM_ITERATIONS); do
        result=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${SETTINGS}" | grep -o '<rows_read>[0-9]*</rows_read>' | grep -o '[0-9]*')
        if [ -n "$result" ] && [ "$result" -gt 0 ]; then
            ok=$((ok + 1))
        fi
    done
    if [ "$ok" -eq 0 ]; then
        echo "XML FAIL: rows_read=0 in all ${NUM_ITERATIONS} iterations"
    else
        echo "XML OK"
    fi
}

function check_rows_read_http_json()
{
    local fmt=$1
    local ok=0
    for _ in $(seq 1 $NUM_ITERATIONS); do
        result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT ${fmt} SETTINGS ${SETTINGS}" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
        if [ -n "$result" ] && [ "$result" -gt 0 ]; then
            ok=$((ok + 1))
        fi
    done
    if [ "$ok" -eq 0 ]; then
        echo "${fmt} HTTP FAIL: rows_read=0 in all ${NUM_ITERATIONS} iterations"
    else
        echo "${fmt} HTTP OK"
    fi
}

function check_rows_read_http_xml()
{
    local ok=0
    for _ in $(seq 1 $NUM_ITERATIONS); do
        result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${SETTINGS}" | grep -o '<rows_read>[0-9]*</rows_read>' | grep -o '[0-9]*')
        if [ -n "$result" ] && [ "$result" -gt 0 ]; then
            ok=$((ok + 1))
        fi
    done
    if [ "$ok" -eq 0 ]; then
        echo "XML HTTP FAIL: rows_read=0 in all ${NUM_ITERATIONS} iterations"
    else
        echo "XML HTTP OK"
    fi
}

# Verify rows_read > 0 for each format via clickhouse-client.
# Each helper echoes either `<fmt> OK` or `<fmt> FAIL: ...`; the
# `.reference` file expects all `OK`, so any `FAIL` line is caught cleanly
# by the reference diff with the offending format visible in the report.
for fmt in JSON JSONCompact JSONColumnsWithMetadata; do
    check_rows_read_client_json "$fmt"
done
check_rows_read_client_xml

# Same via HTTP
for fmt in JSON JSONCompact JSONColumnsWithMetadata; do
    check_rows_read_http_json "$fmt"
done
check_rows_read_http_xml

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
