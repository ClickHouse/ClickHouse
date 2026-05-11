#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database

# Pre-filter selectivity matrix for the `ann` index. For five selectivity buckets
# (0% / 1% / 10% / 50% / 100%) we assert that the ANN-routed query returns the
# same id set as the brute-force baseline. We compare id sets — not distances —
# so the check is robust against SIMD reassociation.
#
# The brute-force baseline is obtained by running each query a second time with
# `SETTINGS try_use_ann_search = 0` on the **outer** SELECT. Inner-subquery
# SETTINGS are silently dropped for query-plan-level options like
# `try_use_ann_search`, so the two baselines must be issued as separate
# top-level queries and compared at the shell level.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_ann_prefilter"

wait_for_full_coverage() {
    local deadline=$((SECONDS + 120))
    while [ $SECONDS -lt $deadline ]; do
        local row
        row=$($CLICKHOUSE_CLIENT -q "SELECT tupleElement(tableANNCoverage(currentDatabase(), '$TABLE'), 'total') = tupleElement(tableANNCoverage(currentDatabase(), '$TABLE'), 'covered')")
        if [ "$row" = "1" ]; then
            return 0
        fi
        sleep 0.2
    done
    echo "TIMEOUT waiting for ANN coverage on $TABLE" >&2
    return 1
}

# Compare ANN id set against brute-force id set for one filter expression.
# Prints `1` if they match, `0` otherwise. Both queries live at the top level
# so the SETTINGS clause on the brute-force query actually disables ANN.
assert_ann_eq_brute() {
    local where_clause="$1"
    local ann_ids brute_ids
    ann_ids=$($CLICKHOUSE_CLIENT -q "
        SELECT arraySort(groupArray(id)) FROM (
            SELECT id FROM $TABLE
            $where_clause
            ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
            LIMIT 10
        )")
    brute_ids=$($CLICKHOUSE_CLIENT -q "
        SELECT arraySort(groupArray(id)) FROM (
            SELECT id FROM $TABLE
            $where_clause
            ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
            LIMIT 10
        ) SETTINGS try_use_ann_search = 0")
    if [ "$ann_ids" = "$brute_ids" ]; then
        echo 1
    else
        echo 0
    fi
}

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS $TABLE;

CREATE TABLE $TABLE
(
    id UInt64,
    bucket UInt8,
    emb Array(Float32),
    INDEX idx_emb emb TYPE ann(dim = 16) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    ann_group_min_rows = 1,
    ann_group_max_rows = 100000,
    ann_group_max_parts = 256;

INSERT INTO $TABLE
SELECT
    number,
    toUInt8(number % 10),
    arrayMap(i -> toFloat32(rand64(number * 16 + i) % 1000) / 1000.0, range(16))
FROM numbers(1000);

SYSTEM BUILD ANN INDEX $TABLE;
"

wait_for_full_coverage

# Selectivity matrix.
# 0%: filter excludes every row.
assert_ann_eq_brute "WHERE bucket >= 100"
# 0%: also assert the result is empty.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM (
        SELECT id FROM $TABLE
        WHERE bucket >= 100
        ORDER BY L2Distance(emb, (SELECT arrayMap(i -> toFloat32(i) / 16.0, range(16))))
        LIMIT 10
    )"

# 1% of 1000 rows.
assert_ann_eq_brute "WHERE id < 10"

# 10%: one bucket out of ten.
assert_ann_eq_brute "WHERE bucket = 0"

# 50%.
assert_ann_eq_brute "WHERE bucket < 5"

# 100%: trivially-true predicate keeps every row.
assert_ann_eq_brute "WHERE bucket < 100"

$CLICKHOUSE_CLIENT -q "DROP TABLE $TABLE"
