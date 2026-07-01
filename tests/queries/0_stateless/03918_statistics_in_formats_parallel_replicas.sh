#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
#
# Regression guard for issue #85785: with parallel replicas and `LIMIT`, the
# `rows_read` statistic emitted in JSON/XML output could be lost (reported as 0
# or under-counted), because the output format wrote the statistics section
# before the pipeline finished collecting the trailing `Progress` packets that
# are drained from the replica connections after the early (`LIMIT`)
# cancellation. The fix writes the statistics only after `finalizeExecution`
# has collected all remaining progress.
#
# This is a forward guard on the fixed behaviour: the `rows_read` reported under
# parallel replicas must match the value reported for the same query without
# parallel replicas (the single-node read has no remote-drain race and is
# therefore correct on any build). Settings randomization is disabled so that
# the table is a single granule and the value is deterministic across runs.
#
# Note: the underlying defect is a rare timing race that does not manifest
# deterministically on idle hardware, so this test guards the fixed behaviour
# rather than acting as a deterministic reproducer of the race (in particular,
# the `Bugfix validation` job cannot be expected to reproduce it in one run).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_NAME="t_03918_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
# 1000 rows fit into a single granule (default index_granularity = 8192), so the
# whole table is read as one unit and rows_read is a single deterministic value,
# independent of how parallel replicas split the work.
$CLICKHOUSE_CLIENT --query="CREATE TABLE ${TABLE_NAME} (number UInt64) ENGINE = MergeTree ORDER BY number"
$CLICKHOUSE_CLIENT --query="INSERT INTO ${TABLE_NAME} SELECT * FROM system.numbers LIMIT 1000"

SETTINGS="enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1"

# Ground truth: rows_read for the same query without parallel replicas. The
# single-node read has no remote-drain race, so this value is correct on any
# build and equals the parallel value on a correct build.
EXPECTED=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT JSON SETTINGS enable_parallel_replicas=0" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')

function check_rows_read_client_json()
{
    local fmt=$1
    local result
    result=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT ${fmt} SETTINGS ${SETTINGS}" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
    if [ "$result" = "$EXPECTED" ]; then
        echo "${fmt} OK"
    else
        echo "${fmt} FAIL: rows_read=${result} (expected ${EXPECTED})"
    fi
}

function check_rows_read_client_xml()
{
    local result
    result=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${SETTINGS}" | grep -o '<rows_read>[0-9]*</rows_read>' | grep -o '[0-9]*')
    if [ "$result" = "$EXPECTED" ]; then
        echo "XML OK"
    else
        echo "XML FAIL: rows_read=${result} (expected ${EXPECTED})"
    fi
}

function check_rows_read_http_json()
{
    local fmt=$1
    local result
    result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT ${fmt} SETTINGS ${SETTINGS}" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
    if [ "$result" = "$EXPECTED" ]; then
        echo "${fmt} HTTP OK"
    else
        echo "${fmt} HTTP FAIL: rows_read=${result} (expected ${EXPECTED})"
    fi
}

function check_rows_read_http_xml()
{
    local result
    result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${SETTINGS}" | grep -o '<rows_read>[0-9]*</rows_read>' | grep -o '[0-9]*')
    if [ "$result" = "$EXPECTED" ]; then
        echo "XML HTTP OK"
    else
        echo "XML HTTP FAIL: rows_read=${result} (expected ${EXPECTED})"
    fi
}

# Verify rows_read matches the single-node value for each format via clickhouse-client.
# Each helper echoes either `<fmt> OK` or `<fmt> FAIL: ...`; the `.reference` file
# expects all `OK`, so any `FAIL` line is caught cleanly by the reference diff with
# the offending format and its rows_read value visible in the report.
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
