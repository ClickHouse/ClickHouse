#!/usr/bin/env bash
# Tags: long

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="test_04039_snapshot_teardown_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE_PROJ="test_04039_proj_teardown_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ITERATIONS=20

function run_query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1" 2>/dev/null
}

function cleanup()
{
    run_query "DROP TABLE IF EXISTS ${TABLE} SYNC" >/dev/null 2>&1 ||:
    run_query "DROP TABLE IF EXISTS ${TABLE_PROJ} SYNC" >/dev/null 2>&1 ||:
}

function wait_for_reading()
{
    local query_id=$1 && shift
    local timeout="${1:-60}"
    local start=$EPOCHSECONDS

    while true; do
        local read_rows
        read_rows=$(run_query "SELECT read_rows FROM system.processes WHERE query_id = '${query_id}'" || echo "")
        if [ -n "$read_rows" ] && [ "$read_rows" -gt 0 ] 2>/dev/null; then
            return 0
        fi

        if [ -z "$read_rows" ]; then
            echo "Query ${query_id} finished before any rows were read" >&2
            return 1
        fi

        if ((EPOCHSECONDS - start > timeout)); then
            echo "Timeout waiting for reading to start for ${query_id}" >&2
            return 1
        fi

        sleep 0.1
    done
}

function run_iteration_proj()
{
    local iteration=$1 && shift
    local query_id="04039_proj_${CLICKHOUSE_TEST_UNIQUE_NAME}_${iteration}"
    local log_file
    log_file=$(mktemp "${CLICKHOUSE_TMP}/04039_proj.XXXXXX.log")

    # Exercise the projection-index read-pool path (MergeTreeReadPoolProjectionIndex)
    # during teardown. The WHERE clause on `bucket` triggers the projection index reader
    # which calls createReaders -> data_part->storage.getSettings().
    $CLICKHOUSE_CLIENT \
        --query_id "$query_id" \
        --format Null \
        --max_threads 1 \
        --max_block_size 1 \
        --enable_shared_storage_snapshot_in_query 0 \
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0 \
        --min_table_rows_to_use_projection_index 0 \
        --query "
        SELECT sleepEachRow(0.01)
        FROM ${TABLE_PROJ}
        WHERE (_part, _part_offset) IN
        (
            SELECT _part, _part_offset
            FROM ${TABLE_PROJ}
            WHERE bucket = 1
        )
    " >/dev/null 2>"$log_file" &
    local pid=$!

    wait_for_query_to_start "$query_id"
    if ! wait_for_reading "$query_id"; then
        run_query "KILL QUERY WHERE query_id = '${query_id}' SYNC FORMAT Null" >/dev/null 2>&1 ||:
        wait "$pid" >/dev/null 2>&1 ||:
        rm -f "$log_file"
        return 1
    fi

    run_query "KILL QUERY WHERE query_id = '${query_id}' SYNC FORMAT Null" >/dev/null 2>&1 ||:
    wait "$pid" >/dev/null 2>&1 ||:

    if grep -F "Code:" "$log_file" | grep -v -F "QUERY_WAS_CANCELLED" >/dev/null; then
        cat "$log_file" >&2
        rm -f "$log_file"
        return 1
    fi

    rm -f "$log_file"
}

function run_iteration()
{
    local iteration=$1 && shift
    local query_id="04039_${CLICKHOUSE_TEST_UNIQUE_NAME}_${iteration}"
    local log_file
    log_file=$(mktemp "${CLICKHOUSE_TMP}/04039.XXXXXX.log")

    # Kill the query after readers are initialized so teardown runs while the
    # pool still owns parts that came from the stripped snapshot.
    $CLICKHOUSE_CLIENT \
        --query_id "$query_id" \
        --format Null \
        --max_threads 1 \
        --max_block_size 1 \
        --enable_shared_storage_snapshot_in_query 0 \
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0 \
        --query "
        SELECT sleepEachRow(0.01)
        FROM ${TABLE}
        WHERE (_part, _part_offset) IN
        (
            SELECT _part, _part_offset
            FROM ${TABLE}
            WHERE bucket = 1
        )
    " >/dev/null 2>"$log_file" &
    local pid=$!

    wait_for_query_to_start "$query_id"
    if ! wait_for_reading "$query_id"; then
        run_query "KILL QUERY WHERE query_id = '${query_id}' SYNC FORMAT Null" >/dev/null 2>&1 ||:
        wait "$pid" >/dev/null 2>&1 ||:
        rm -f "$log_file"
        return 1
    fi

    run_query "KILL QUERY WHERE query_id = '${query_id}' SYNC FORMAT Null" >/dev/null 2>&1 ||:
    wait "$pid" >/dev/null 2>&1 ||:

    if grep -F "Code:" "$log_file" | grep -v -F "QUERY_WAS_CANCELLED" >/dev/null; then
        cat "$log_file" >&2
        rm -f "$log_file"
        return 1
    fi

    rm -f "$log_file"
}

trap cleanup EXIT

cleanup

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${TABLE}
    (
        key UInt64,
        bucket UInt64,
        value String
    )
    ENGINE = MergeTree
    PARTITION BY bucket
    ORDER BY (bucket, key)
    SETTINGS index_granularity = 1
"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES ${TABLE}"

for i in {0..31}; do
    bucket=$((i % 8))
    offset=$((i * 256))

    $CLICKHOUSE_CLIENT --query "
        INSERT INTO ${TABLE}
        SELECT
            number + ${offset},
            ${bucket},
            toString(number + ${offset})
        FROM numbers(256)
    "
done

for iteration in $(seq 1 "${ITERATIONS}"); do
    run_iteration "$iteration"
done

$CLICKHOUSE_CLIENT --query "SELECT count() = 8192 FROM ${TABLE}"

# -----------------------------------------------------------------------
# Part 2: projection-index read-pool teardown race
# Exercises the MergeTreeReadPoolProjectionIndex path where createReaders
# dereferences data_part->storage.getSettings() on projection parts.
# -----------------------------------------------------------------------

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${TABLE_PROJ}
    (
        key UInt64,
        bucket UInt64,
        value String,
        PROJECTION bucket_proj INDEX bucket TYPE basic
    )
    ENGINE = MergeTree
    PARTITION BY bucket
    ORDER BY (bucket, key)
    SETTINGS
        index_granularity = 1,
        min_bytes_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_vertical_merge_algorithm = 0
"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES ${TABLE_PROJ}"

for i in {0..31}; do
    bucket=$((i % 8))
    offset=$((i * 256))

    $CLICKHOUSE_CLIENT --query "
        INSERT INTO ${TABLE_PROJ}
        SELECT
            number + ${offset},
            ${bucket},
            toString(number + ${offset})
        FROM numbers(256)
    "
done

for iteration in $(seq 1 "${ITERATIONS}"); do
    run_iteration_proj "$iteration"
done

$CLICKHOUSE_CLIENT --query "SELECT count() = 8192 FROM ${TABLE_PROJ}"
