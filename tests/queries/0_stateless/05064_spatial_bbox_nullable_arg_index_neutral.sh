#!/usr/bin/env bash
# A `Nullable` argument can make a geometry function's `Unknown geometry type` exception disappear when
# a sibling conjunct filters the block empty: `defaultImplementationForNulls` returns an empty result
# for `input_rows_count == 0` before `executeImpl` runs, so `callOnTwoGeometryDataTypes` never gets to
# raise. That is a trunk defect, tracked in https://github.com/ClickHouse/ClickHouse/issues/117208, and
# whether it bites depends on how the planner arranges the filter steps.
#
# What the `spatial_bbox` index owes is only that it does not CAUSE it: a granule bbox is a
# conservative superset, so it can prune only granules the sibling conjunct would have filtered anyway.
# So this test asserts nothing about WHICH outcome a shape produces -- that varies with planner
# settings and is not this index's business. It runs each shape three ways (index enabled, index
# disabled, and against a table declaring no index) and requires the three answers to be identical,
# exception text included. That invariant holds under any randomized settings.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS test_spatial_bbox_nullable_arg;
DROP TABLE IF EXISTS test_spatial_bbox_nullable_arg_no_index;

CREATE TABLE test_spatial_bbox_nullable_arg
(
    poly Polygon,
    n Nullable(UInt8),
    INDEX idx_bbox poly TYPE spatial_bbox GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

CREATE TABLE test_spatial_bbox_nullable_arg_no_index
(
    poly Polygon,
    n Nullable(UInt8)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO test_spatial_bbox_nullable_arg VALUES ([[(100., 100.), (101., 100.), (101., 101.), (100., 100.)]], 1);
INSERT INTO test_spatial_bbox_nullable_arg_no_index VALUES ([[(100., 100.), (101., 100.), (101., 101.), (100., 100.)]], 1);
"

# Normalise so only the outcome is compared, not which table produced it.
run() {
    local table="$1" predicate="$2" extra="$3"
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM $table
        WHERE pointInPolygon((0., 0.), poly) AND $predicate
        SETTINGS short_circuit_function_evaluation = 'disable', optimize_move_to_prewhere = 0${extra:+, $extra};
    " 2>&1 | sed -e 's/DB::Exception: Received from [^.]*\. //' -e "s/${table}/TABLE/g" | head -1
}

check() {
    local label="$1" predicate="$2"
    local with_index without_index no_index_table
    with_index=$(run test_spatial_bbox_nullable_arg "$predicate" "use_skip_indexes = 1")
    without_index=$(run test_spatial_bbox_nullable_arg "$predicate" "use_skip_indexes = 0")
    no_index_table=$(run test_spatial_bbox_nullable_arg_no_index "$predicate" "")

    if [ "$with_index" = "$without_index" ] && [ "$with_index" = "$no_index_table" ]; then
        echo "$label: index-neutral"
    else
        echo "$label: DIVERGED"
        echo "  index on:  $with_index"
        echo "  index off: $without_index"
        echo "  no index:  $no_index_table"
    fi
}

# A sibling `Nullable(UInt8)` column.
check "nullable sibling column" "polygonsIntersectCartesian(n, [[(10., 10.), (11., 10.), (11., 11.), (10., 10.)]])"

# A `Nullable` non-geometry constant in an otherwise well-typed call.
check "nullable constant" "polygonsIntersectCartesian(poly, CAST(1 AS Nullable(UInt8)))"

# A `Nullable(FixedString(N))` constant.
check "nullable fixed string constant" "polygonsIntersectCartesian(poly, CAST('ab' AS Nullable(FixedString(2))))"

# Without any `Nullable` wrapper the exception always surfaces, index or not.
check "plain non-geometry constant" "polygonsIntersectCartesian(poly, 1)"

$CLICKHOUSE_CLIENT -q "
DROP TABLE test_spatial_bbox_nullable_arg_no_index;
DROP TABLE test_spatial_bbox_nullable_arg;
"
