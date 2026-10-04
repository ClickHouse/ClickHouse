#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel-replicas, no-flaky-check
# Test for https://github.com/ClickHouse/ClickHouse/issues/113003
# Skip-index condition building matched a filter column name against JSONAllPaths(...) index
# columns by enumerating every dot split of the name and formatting a lookup key per split. The
# name embeds the text of a folded constant, so a large dotted constant made index analysis
# quadratic in the constant's length.
# Each arm compares a dotted constant against a no-dots constant of the same length. The oracle is
# the allocated-bytes counter rather than wall clock: it is exactly reproducible, whereas the
# ~2.6s planning delta is smaller than debug-build client startup jitter.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

REPEATS=100000
# Measured on a debug build: pre-fix the dotted arm allocates 2.8x-3.0x the control in every arm,
# post-fix 1.0003x. 1.5 sits an order of magnitude away from the fixed behavior and ~1.9x below
# the regression, so it discriminates with margin on both sides.
MAX_RATIO_PERCENT=150

LONG_SUFFIX=$(printf 'a%.0s' $(seq 1 170))

$CLICKHOUSE_CLIENT -nm -q "
    SET enable_json_type = 1, allow_suspicious_indices = 1;

    -- index_granularity is pinned on every table: the granule counts asserted below are
    -- ceil(rows / index_granularity), which the test runner otherwise randomizes.

    -- No JSON column at all, so the matcher can never succeed: the reported shape.
    CREATE TABLE plain (s String, INDEX ix s TYPE bloom_filter GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

    -- index_columns carries a long NON-JSON entry. Bounding the split enumeration by the longest
    -- index column would leave this arm quadratic.
    CREATE TABLE longidx (s String, INDEX ilong concat(s, '${LONG_SUFFIX}') TYPE bloom_filter GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

    -- A JSONAllPaths index IS present, so an early-out keyed on its absence cannot fire.
    CREATE TABLE withjson (s String, j JSON,
        INDEX ix s TYPE bloom_filter GRANULARITY 1,
        INDEX jx JSONAllPaths(j) TYPE bloom_filter GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

    -- Token indexes reach the same matcher through a different condition class.
    CREATE TABLE tokens (s String, INDEX ix s TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

    INSERT INTO plain SELECT 'v' || toString(number % 10) FROM numbers(1000);
    INSERT INTO longidx SELECT 'v' || toString(number % 10) FROM numbers(1000);
    INSERT INTO withjson SELECT 'v' || toString(number % 10), '{\"a\":1}' FROM numbers(1000);
    INSERT INTO tokens SELECT 'v' || toString(number % 10) FROM numbers(1000);
"

# Sets ALLOC_BYTES to the minimum bytes the server allocated while planning one query, over
# several identical runs. EXPLAIN runs index analysis without reading data, so the measurement
# is the matcher's cost.
# A single run is not a stable oracle: whichever query happens to be the first to touch a cache
# or spin up a thread pool absorbs a transient multi-megabyte allocation, and the dotted arm
# always runs first in the loop below, so such a one-off inflates the ratio arbitrarily
# (observed in CI: a 4216780-byte dotted arm against a 22224-byte control). A genuine quadratic
# regression is deterministic and shows up in every run, so the minimum keeps discriminating.
ALLOC_RUNS=3
alloc_bytes_for() {
    local table="$1" unit="$2"
    local query_id_prefix="04780-${CLICKHOUSE_DATABASE}-${table}-${unit}-${RANDOM}"
    local run
    for run in $(seq 1 $ALLOC_RUNS); do
        $CLICKHOUSE_CLIENT --query_id "${query_id_prefix}-${run}" --max_query_size 1048576 --max_execution_time 300 -q "
            SELECT count() FROM (
                EXPLAIN indexes = 1
                SELECT count() FROM ${table} WHERE position(repeat('${unit}', ${REPEATS}), s) = 1
            )" >/dev/null
    done
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log" >/dev/null
    ALLOC_BYTES=$($CLICKHOUSE_CLIENT -q "
        SELECT min(ProfileEvents['MemoryAllocatedWithoutCheckBytes'])
        FROM system.query_log
        WHERE current_database = currentDatabase() AND startsWith(query_id, '${query_id_prefix}-') AND type = 'QueryFinish'
        HAVING count() = ${ALLOC_RUNS}")
    [ -n "$ALLOC_BYTES" ] && [ "$ALLOC_BYTES" -gt 0 ]
}

for table in plain longidx withjson tokens; do
    alloc_bytes_for "$table" 'a.' || { echo "FAIL: no query_log row for $table dotted arm" >&2; exit 1; }
    dotted=$ALLOC_BYTES

    alloc_bytes_for "$table" 'ab' || { echo "FAIL: no query_log row for $table control arm" >&2; exit 1; }
    control=$ALLOC_BYTES

    if [ $((dotted * 100)) -gt $((control * MAX_RATIO_PERCENT)) ]; then
        echo "FAIL: $table index analysis over a constant with ${REPEATS} dots allocated ${dotted} bytes," \
             "more than ${MAX_RATIO_PERCENT}% of the no-dots control (${control} bytes)" >&2
        exit 1
    fi
    echo "$table OK"
done

# A dotted constant must not change which granules are read, and a real JSON subcolumn filter on
# the same table must still prune.
$CLICKHOUSE_CLIENT -nm -q "
    SET enable_json_type = 1;
    SELECT count() FROM withjson WHERE position(repeat('a.', 100), s) = 1;
    SELECT trimLeft(explain) FROM (
        EXPLAIN indexes = 1 SELECT count() FROM withjson WHERE j.absent_path = 'zzz'
    ) WHERE explain LIKE '%Granules:%';
"

$CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE plain; DROP TABLE longidx; DROP TABLE withjson; DROP TABLE tokens;
"
