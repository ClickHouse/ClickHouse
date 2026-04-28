#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database

# Verify the two ANN benchmarking knobs:
#   - `vector_search_unindexed_metric_source = 'index'` makes the unindexed-parts dispatch
#     borrow DiskANN's distance kernel via `IANNIndexSearcher::computeDistances`, so that
#     a query mixing indexed and unindexed parts uses one consistent SIMD kernel for both.
#   - `vector_search_force_brute_force = 1` skips the table-level ANN index lookup and
#     pushes every part through the unindexed dispatch. Combined with the setting above
#     this gives an "index-kernel brute force" baseline for benchmarks.
#
# The cross-check in every case is correctness: top-K returned by the new code path must
# match the exact answer produced by the SQL distance function with `try_use_ann_search = 0`.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_ann_metric_source_$$"

wait_for_full_coverage() {
    local table=$1
    local deadline=$((SECONDS + 120))
    while [ $SECONDS -lt $deadline ]; do
        local row
        row=$($CLICKHOUSE_CLIENT -q "SELECT tupleElement(tableANNCoverage(currentDatabase(), '$table'), 'total') = tupleElement(tableANNCoverage(currentDatabase(), '$table'), 'covered')")
        if [ "$row" = "1" ]; then
            return 0
        fi
        sleep 0.2
    done
    echo "TIMEOUT waiting for ANN coverage on $table" >&2
    return 1
}

QUERY_VEC="[0.5, 0.5, 0.5, 0.5]::Array(Float32)"

# Baseline answer: full scan with `try_use_ann_search = 0` so neither the index nor the
# unindexed-fast-path runs. Acts as the ground truth for top-K equality checks.
baseline_topk() {
    local table=$1
    $CLICKHOUSE_CLIENT -q "
        SELECT groupArray(id) FROM
        (
            SELECT id FROM $table
            ORDER BY L2Distance(emb, $QUERY_VEC)
            LIMIT 5
            SETTINGS try_use_ann_search = 0
        )"
}

# Answer under the configuration we are validating.
under_test_topk() {
    local table=$1
    local extra_settings=$2
    $CLICKHOUSE_CLIENT -q "
        SELECT groupArray(id) FROM
        (
            SELECT id FROM $table
            ORDER BY L2Distance(emb, $QUERY_VEC)
            LIMIT 5
            SETTINGS $extra_settings
        )"
}

assert_topk_equals_baseline() {
    local label=$1
    local table=$2
    local extra_settings=$3
    local expected actual
    expected=$(baseline_topk "$table")
    actual=$(under_test_topk "$table" "$extra_settings")
    if [ "$expected" = "$actual" ]; then
        echo "$label: OK"
    else
        echo "$label: MISMATCH"
        echo "  expected: $expected"
        echo "  actual:   $actual"
    fi
}

$CLICKHOUSE_CLIENT -m <<EOSQL
DROP TABLE IF EXISTS $TABLE;

CREATE TABLE $TABLE
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4, metric = 'L2') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    -- Keep the background ANN builder away from the second batch so the test can pin the
    -- mixed-table scenario (one indexed part + one unindexed part). We trigger the explicit
    -- build below; SYSTEM BUILD ANN INDEX ignores ann_group_min_rows for the first batch.
    ann_group_min_rows = 100000;

-- First batch is built into the index.
INSERT INTO $TABLE SELECT number, arrayMap(i -> toFloat32((number + i) % 7), range(4)) FROM numbers(64);
SYSTEM BUILD ANN INDEX $TABLE;
EOSQL

wait_for_full_coverage "$TABLE"

# Add a second batch AFTER the build so the table now has both indexed parts (the first
# batch, covered by the build) and an unindexed part (the second batch).
$CLICKHOUSE_CLIENT -q "INSERT INTO $TABLE SELECT 1000 + number, arrayMap(i -> toFloat32((number + i) % 7) + 0.25, range(4)) FROM numbers(32)"

# (1) Default: source=sql, force=0. Mixed table, indexed parts use DiskANN kernel,
# unindexed parts use SQL `L2Distance`. Top-K must equal the baseline.
assert_topk_equals_baseline "default (source=sql)" \
    "$TABLE" "vector_search_unindexed_metric_source = 'sql'"

# (2) source=index, force=0. Mixed table, unindexed parts use the same DiskANN kernel as
# the indexed parts. Top-K must still equal the baseline (the kernel difference between
# squared-L2 and L2 only affects absolute values, not ordering).
assert_topk_equals_baseline "mixed table, source=index" \
    "$TABLE" "vector_search_unindexed_metric_source = 'index'"

# (3) force=1, source=sql. All parts go through the unindexed dispatch, distances come
# from SQL `L2Distance`. Top-K must equal the baseline.
assert_topk_equals_baseline "force_brute_force=1, source=sql" \
    "$TABLE" "vector_search_force_brute_force = 1, vector_search_unindexed_metric_source = 'sql'"

# (4) force=1, source=index. All parts go through the unindexed dispatch, distances come
# from the DiskANN kernel. Top-K must equal the baseline.
assert_topk_equals_baseline "force_brute_force=1, source=index" \
    "$TABLE" "vector_search_force_brute_force = 1, vector_search_unindexed_metric_source = 'index'"

# Zero-group fallback: a fresh table with an ANN index but no built groups must not raise
# when `source=index` is set — the unindexed dispatch should silently fall back to SQL.
TABLE_FRESH="t_ann_fresh_$$"
$CLICKHOUSE_CLIENT -m <<EOSQL
DROP TABLE IF EXISTS $TABLE_FRESH;
CREATE TABLE $TABLE_FRESH
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4, metric = 'L2') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, ann_group_min_rows = 100000;

INSERT INTO $TABLE_FRESH SELECT number, arrayMap(i -> toFloat32((number + i) % 5), range(4)) FROM numbers(16);
EOSQL

assert_topk_equals_baseline "zero-group, force=1 source=index falls back" \
    "$TABLE_FRESH" "vector_search_force_brute_force = 1, vector_search_unindexed_metric_source = 'index'"

$CLICKHOUSE_CLIENT -q "DROP TABLE $TABLE"
$CLICKHOUSE_CLIENT -q "DROP TABLE $TABLE_FRESH"
