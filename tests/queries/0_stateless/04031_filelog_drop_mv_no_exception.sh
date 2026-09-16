#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: FileLog requires inotify
# Tag no-parallel: uses a shared USER_FILES_PATH directory

# Regression test for DEPENDENCIES_NOT_FOUND exception thrown when a
# materialized view is dropped while the FileLog background thread is
# streaming data. There is a TOCTOU race between checkDependencies
# (which sees the MV) and collectAllDependencies inside
# InsertDependenciesBuilder (which no longer sees it).
# We loop several times to increase the chance of hitting the race window.

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DATA_DIR}"
rm -rf "${DATA_DIR:?}"/*

# Capture start time to scope the text_log check to this test run only
TEST_START=$(${CLICKHOUSE_CLIENT} --query "SELECT now()")

for iteration in {1..5}; do
    # Prepare a data file for FileLog to consume
    for i in {1..10}; do
        echo "${i},${i}"
    done > "${DATA_DIR}/data.csv"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.filelog_src SYNC"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.filelog_mv SYNC"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.filelog_dst SYNC"

    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${CLICKHOUSE_DATABASE}.filelog_dst (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    "
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${CLICKHOUSE_DATABASE}.filelog_src (k UInt64, v UInt64)
        ENGINE = FileLog('${DATA_DIR}/', 'CSV')
    "
    ${CLICKHOUSE_CLIENT} --query "
        CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.filelog_mv TO ${CLICKHOUSE_DATABASE}.filelog_dst
        AS SELECT * FROM ${CLICKHOUSE_DATABASE}.filelog_src
    "

    # Wait until the background thread picks up some data
    for attempt in {1..30}; do
        count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.filelog_dst")
        [[ "${count}" -ge 10 ]] && break
        sleep 0.2
    done

    if [[ "${count}" -lt 10 ]]; then
        echo "Warm-up failed: only ${count} rows consumed after 30 attempts" >&2
        exit 1
    fi

    # Drop the MV while writing new data to trigger the race:
    # the background thread may have already passed checkDependencies
    # but not yet built the insert pipeline.
    for i in {11..20}; do
        echo "${i},${i}"
    done >> "${DATA_DIR}/data.csv"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.filelog_mv SYNC"

    # Give the background thread time to attempt streaming with no MV
    sleep 0.2

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.filelog_src SYNC"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.filelog_dst SYNC"

    # Clean data files for the next iteration
    rm -rf "${DATA_DIR:?}"/*
done

rm -rf "${DATA_DIR:?}"

# Verify that no DEPENDENCIES_NOT_FOUND exception was thrown in the background.
# Before the fix, the FileLog background thread would throw this exception
# when the materialized view was dropped during streaming.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() == 0
    FROM system.text_log
    WHERE event_date >= yesterday()
        AND event_time >= '${TEST_START}'
        AND level = 'Error'
        AND message LIKE '%DEPENDENCIES_NOT_FOUND%'
        AND message LIKE '%filelog_src%'
    SETTINGS max_rows_to_read = 0
"
