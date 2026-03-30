#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_NAME="t_03918_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
$CLICKHOUSE_CLIENT --query="CREATE TABLE ${TABLE_NAME} (number UInt64) ENGINE = MergeTree ORDER BY number"
$CLICKHOUSE_CLIENT --query="INSERT INTO ${TABLE_NAME} SELECT * FROM system.numbers LIMIT 1000"

SETTINGS="enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1"

# Run each check multiple times to reliably detect the race condition on master
# (where rows_read can be 0 due to the race between format finalization and progress
# collection). With the fix, all iterations should pass deterministically.
NUM_ITERATIONS=10

function check_rows_read_client_json()
{
    local fmt=$1
    for _ in $(seq 1 $NUM_ITERATIONS); do
        result=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT ${fmt} SETTINGS ${SETTINGS}" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
        if [ -z "$result" ] || [ "$result" -eq 0 ]; then
            echo "${fmt} FAIL: rows_read=${result}"
            return
        fi
    done
    echo "${fmt} OK"
}

function check_rows_read_client_xml()
{
    for _ in $(seq 1 $NUM_ITERATIONS); do
        result=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${SETTINGS}" | grep -o '<rows_read>[0-9]*</rows_read>' | grep -o '[0-9]*')
        if [ -z "$result" ] || [ "$result" -eq 0 ]; then
            echo "XML FAIL: rows_read=${result}"
            return
        fi
    done
    echo "XML OK"
}

function check_rows_read_http_json()
{
    local fmt=$1
    for _ in $(seq 1 $NUM_ITERATIONS); do
        result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT ${fmt} SETTINGS ${SETTINGS}" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
        if [ -z "$result" ] || [ "$result" -eq 0 ]; then
            echo "${fmt} HTTP FAIL: rows_read=${result}"
            return
        fi
    done
    echo "${fmt} HTTP OK"
}

function check_rows_read_http_xml()
{
    for _ in $(seq 1 $NUM_ITERATIONS); do
        result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${SETTINGS}" | grep -o '<rows_read>[0-9]*</rows_read>' | grep -o '[0-9]*')
        if [ -z "$result" ] || [ "$result" -eq 0 ]; then
            echo "XML HTTP FAIL: rows_read=${result}"
            return
        fi
    done
    echo "XML HTTP OK"
}

# Verify rows_read > 0 for each format via clickhouse-client
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
