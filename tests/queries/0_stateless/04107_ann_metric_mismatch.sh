#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database

# Verify that the ANN optimizer (`useANNSearch`) refuses to route a query through the index
# when the query's distance function does not match the index's metric. The DDL only allows
# `metric ∈ {L2, Cosine}` while the optimizer otherwise accepts L2Distance / cosineDistance /
# dotProduct, so without this guard a Cosine-only index could silently answer an L2 query
# (or vice versa) with semantically wrong results. `dotProduct` has no corresponding metric
# at all and must always be rejected. On rejection the plan falls back to a full scan, which
# is the same fallback used when no index is present.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_L2="t_ann_mismatch_l2"
TABLE_COS="t_ann_mismatch_cos"

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

# `_distance` appears in the plan iff the optimizer rewrote the ORDER BY into the index path.
# The original `FUNCTION <distance_function>` node remains iff the rewrite did NOT fire.
# The two assertions are complementary so a passing test pins down both sides of the rewrite.
plan_check() {
    local table=$1
    local distance_function=$2
    local sort_dir=$3
    local expect_distance=$4   # "1" if the rewrite must fire, "0" otherwise
    local expect_function=$5   # "1" if the FUNCTION node must remain, "0" otherwise
    $CLICKHOUSE_CLIENT -q "
        SELECT
            countIf(position(line, '_distance') > 0) > 0 = $expect_distance,
            countIf(position(line, 'FUNCTION $distance_function') > 0) > 0 = $expect_function
        FROM
        (
            SELECT explain AS line FROM
            (
                EXPLAIN PLAN actions = 1
                SELECT id FROM $table
                ORDER BY $distance_function(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32)) $sort_dir
                LIMIT 5
            )
        )"
}

$CLICKHOUSE_CLIENT -m <<EOSQL
DROP TABLE IF EXISTS $TABLE_L2;
DROP TABLE IF EXISTS $TABLE_COS;

CREATE TABLE $TABLE_L2
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
    ann_group_min_rows = 1;

CREATE TABLE $TABLE_COS
(
    id UInt64,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 4, metric = 'Cosine') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    ann_group_min_rows = 1;

INSERT INTO $TABLE_L2  SELECT number, arrayMap(i -> toFloat32((number + i) % 7), range(4)) FROM numbers(64);
INSERT INTO $TABLE_COS SELECT number, arrayMap(i -> toFloat32((number + i) % 7) + 1.0, range(4)) FROM numbers(64);

SYSTEM BUILD ANN INDEX $TABLE_L2;
SYSTEM BUILD ANN INDEX $TABLE_COS;
EOSQL

wait_for_full_coverage "$TABLE_L2"
wait_for_full_coverage "$TABLE_COS"

echo "-- L2 index, matching distance function: rewrite fires"
plan_check "$TABLE_L2" "L2Distance" "ASC" 1 0

echo "-- L2 index, cosineDistance: rewrite must NOT fire (metric mismatch)"
plan_check "$TABLE_L2" "cosineDistance" "ASC" 0 1

echo "-- L2 index, dotProduct: rewrite must NOT fire (no IP metric exists)"
plan_check "$TABLE_L2" "dotProduct" "DESC" 0 1

echo "-- Cosine index, matching distance function: rewrite fires"
plan_check "$TABLE_COS" "cosineDistance" "ASC" 1 0

echo "-- Cosine index, L2Distance: rewrite must NOT fire (metric mismatch)"
plan_check "$TABLE_COS" "L2Distance" "ASC" 0 1

echo "-- Cosine index, dotProduct: rewrite must NOT fire (no IP metric exists)"
plan_check "$TABLE_COS" "dotProduct" "DESC" 0 1

# End-to-end correctness: even when the optimizer skips the index, the user must still get
# the exact answer. Compare the mismatched-distance result against the same query with the
# ANN search explicitly disabled.
echo "-- L2 table answered with cosineDistance returns the exact (full-scan) top-5"
$CLICKHOUSE_CLIENT -q "
    SELECT
        (SELECT groupArray(id) FROM (SELECT id FROM $TABLE_L2 ORDER BY cosineDistance(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32)) LIMIT 5))
        =
        (SELECT groupArray(id) FROM (SELECT id FROM $TABLE_L2 ORDER BY cosineDistance(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32)) LIMIT 5 SETTINGS try_use_ann_search = 0))"

echo "-- Cosine table answered with L2Distance returns the exact (full-scan) top-5"
$CLICKHOUSE_CLIENT -q "
    SELECT
        (SELECT groupArray(id) FROM (SELECT id FROM $TABLE_COS ORDER BY L2Distance(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32)) LIMIT 5))
        =
        (SELECT groupArray(id) FROM (SELECT id FROM $TABLE_COS ORDER BY L2Distance(emb, [0.0, 1.0, 2.0, 3.0]::Array(Float32)) LIMIT 5 SETTINGS try_use_ann_search = 0))"

$CLICKHOUSE_CLIENT -q "DROP TABLE $TABLE_L2"
$CLICKHOUSE_CLIENT -q "DROP TABLE $TABLE_COS"
