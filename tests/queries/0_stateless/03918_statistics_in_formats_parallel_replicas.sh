#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_NAME="t_03918_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
$CLICKHOUSE_CLIENT --query="CREATE TABLE ${TABLE_NAME} (number UInt64) ENGINE = MergeTree ORDER BY number"
$CLICKHOUSE_CLIENT --query="INSERT INTO ${TABLE_NAME} SELECT * FROM system.numbers LIMIT 1000"

SETTINGS="enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1"

# The two-phase finalization mechanism in the pipeline executor guarantees that
# statistics (rows_read, bytes_read) are written after all progress has been
# collected, including progress from parallel replica connection draining.
# No retries are needed.

function check_rows_read_client_json()
{
    local fmt=$1
    result=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT ${fmt} SETTINGS ${SETTINGS}" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
    if [ -n "$result" ] && [ "$result" -gt 0 ]; then
        echo "${fmt} OK"
    else
        echo "${fmt} FAIL: rows_read=${result}"
    fi
}

function check_rows_read_client_xml()
{
    result=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${SETTINGS}" | grep -o '<rows_read>[0-9]*</rows_read>' | grep -o '[0-9]*')
    if [ -n "$result" ] && [ "$result" -gt 0 ]; then
        echo "XML OK"
    else
        echo "XML FAIL: rows_read=${result}"
    fi
}

function check_rows_read_http_json()
{
    local fmt=$1
    result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT ${fmt} SETTINGS ${SETTINGS}" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
    if [ -n "$result" ] && [ "$result" -gt 0 ]; then
        echo "${fmt} HTTP OK"
    else
        echo "${fmt} HTTP FAIL: rows_read=${result}"
    fi
}

function check_rows_read_http_xml()
{
    result=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT XML SETTINGS ${SETTINGS}" | grep -o '<rows_read>[0-9]*</rows_read>' | grep -o '[0-9]*')
    if [ -n "$result" ] && [ "$result" -gt 0 ]; then
        echo "XML HTTP OK"
    else
        echo "XML HTTP FAIL: rows_read=${result}"
    fi
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
