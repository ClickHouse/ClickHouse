#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Native` serializes aggregate states through `IAggregateFunction::serializeBatch`, `RowBinary` one
# state at a time through `IAggregateFunction::serialize`. Both must emit exactly the same bytes.
# `serializeBinaryBulk` writes nothing but the states, so they are the trailing bytes of a
# single-block `Native` stream and can be compared against the whole `RowBinary` one.

WORK_DIR=${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

STATES=()

# Collected and issued in one client invocation: each case is only 1000 rows, so process startup
# would otherwise dominate the test.
check_state()
{
    STATES+=("$1")
}

run_checks()
{
    local query_file="$WORK_DIR/queries.sql"
    : > "$query_file"
    local i=0
    for state_expr in "${STATES[@]}"; do
        local query="SELECT ${state_expr} AS state FROM numbers(1000) GROUP BY number % 97 ORDER BY number % 97"
        echo "${query} INTO OUTFILE '$WORK_DIR/$i.rowbinary' TRUNCATE FORMAT RowBinary;" >> "$query_file"
        echo "${query} INTO OUTFILE '$WORK_DIR/$i.native' TRUNCATE FORMAT Native;" >> "$query_file"
        i=$((i + 1))
    done

    $CLICKHOUSE_CLIENT --max_block_size 100000 --queries-file "$query_file"

    i=0
    for state_expr in "${STATES[@]}"; do
        local size
        if [ ! -s "$WORK_DIR/$i.rowbinary" ] || [ ! -s "$WORK_DIR/$i.native" ]; then
            # One failing query aborts the rest of the batch, so say so rather than report a mismatch.
            echo "$state_expr QUERY FAILED"
        else
            size=$(stat -c %s "$WORK_DIR/$i.rowbinary")
            if tail -c "$size" "$WORK_DIR/$i.native" | cmp -s - "$WORK_DIR/$i.rowbinary"; then
                echo "$state_expr bytes identical"
            else
                echo "$state_expr BYTES DIFFER"
            fi
        fi
        i=$((i + 1))
    done
}

# Fixed-layout states, which the batch path may special-case. `sum` picks a different accumulator
# per argument type, so narrow and wide integers, floats and decimals are all covered.
check_state "sumState(toUInt8(number))"
check_state "sumState(toInt64(number))"
check_state "sumState(number)"
check_state "sumState(toInt128(number))"
check_state "sumState(toUInt256(number))"
check_state "sumState(toNullable(toInt64(number)))"
check_state "sumState(toFloat32(number))"
check_state "sumState(toFloat64(number))"
check_state "sumState(toBFloat16(number))"
check_state "sumState(toDecimal32(number, 2))"
check_state "sumState(toDecimal64(number, 3))"
check_state "sumState(toDecimal128(number, 4))"
check_state "sumState(toDecimal256(number, 5))"
check_state "sumKahanState(toFloat32(number))"
check_state "sumKahanState(toFloat64(number))"
check_state "sumWithOverflowState(toInt64(number))"
check_state "sumWithOverflowState(toInt8(number))"
check_state "minState(toInt64(number))"
check_state "maxState(toInt32(number))"
check_state "anyState(toNullable(toInt8(number)))"
check_state "anyLastState(number)"
check_state "minState(toDecimal64(number, 3))"
check_state "minState(toDate(number))"
check_state "minState(toDateTime(number))"
check_state "maxState(toUInt8(number))"
check_state "maxState(toFloat64(number))"
check_state "maxState(toDecimal128(number, 4))"
check_state "anyState(toInt256(number))"
check_state "anyLastState(toBFloat16(number))"
# Empty states serialize to just the has-value flag, the short end of the size bound.
check_state "minIfState(toInt64(number), number < 0)"
check_state "maxIfState(toFloat64(number), number < 0)"
check_state "countState()"
check_state "avgState(number)"
check_state "avgWeightedState(number, number + 1)"
check_state "argMinState(number, toInt64(-number))"
check_state "argMaxState(number, toInt64(-number))"
check_state "varSampState(toFloat64(number))"
check_state "corrState(toFloat64(number), toFloat64(number * 2))"

# Combinators that forward the bound and the memory writer to their nested function, so these take
# the batch path too: -If/-Array/-Merge delegate unchanged, -Null prefixes a
# flag byte and -OrNull/-OrDefault append one.
check_state "sumIfState(toInt64(number), number % 3 = 0)"
check_state "countIfState(number % 3 = 0)"
check_state "avgIfState(number, number % 3 = 0)"
check_state "avgState(toNullable(number))"
check_state "countState(toNullable(number))"
check_state "sumArrayState([toInt64(number), toInt64(number + 1)])"
check_state "sumMergeState(initializeAggregation('sumState', toInt64(number)))"
check_state "sumOrNullState(toInt64(number))"
check_state "sumOrDefaultState(toInt64(number))"
check_state "sumIfState(toNullable(toInt64(number)), number % 3 = 0)"

# Combinators whose state size is not bounded keep the generic path.
check_state "sumForEachState([toInt64(number), toInt64(number + 1)])"
check_state "sumResampleState(0, 10, 1)(toInt64(number), number % 10)"
check_state "sumDistinctState(toInt64(number))"
check_state "sumMapState([number % 7], [number])"

# Variable-size states must keep using the generic per-state path.
check_state "minState(toString(number))"
check_state "anyState([number, number + 1])"
check_state "uniqState(number)"
check_state "uniqExactState(number)"
check_state "uniqCombinedState(number)"
check_state "groupArrayState(number)"
check_state "quantileState(toFloat64(number))"
check_state "quantilesTDigestState(0.5)(toFloat64(number))"
check_state "groupBitmapState(toUInt32(number))"
check_state "maxMapState([number % 7], [number])"

run_checks

# The batch path must respect the requested state version, not just the default.
for version in 0 1; do
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS versioned;
        CREATE TABLE versioned (k UInt8, s AggregateFunction(${version}, sumMap, Array(UInt64), Array(UInt64)))
        ENGINE = MergeTree ORDER BY k;
        INSERT INTO versioned
        SELECT number % 97, sumMapState([toUInt64(number % 7)], [number]) FROM numbers(1000) GROUP BY number % 97;
        SELECT 'sumMap version ${version}', sumMapMerge(s) FROM versioned;
        DROP TABLE versioned;"
done

# States written to a part, read back and merged, must equal direct aggregation. Insert block sizes
# of 1 and 3 force short batches, exercising the boundary of the batch path.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS agg_roundtrip;
    CREATE TABLE agg_roundtrip
    (
        k UInt16,
        s_sum AggregateFunction(sum, Int64),
        s_str AggregateFunction(min, String),
        s_cnt AggregateFunction(count)
    )
    ENGINE = MergeTree ORDER BY k;"

# Small block sizes would otherwise make one part per group, so keep those runs narrow.
for block_size in 1 3; do
    $CLICKHOUSE_CLIENT --max_insert_block_size "$block_size" --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 --query "
        INSERT INTO agg_roundtrip
        SELECT number % 5, sumState(toInt64(number)), minState(toString(number)), countState()
        FROM numbers(10) GROUP BY number % 5"
done

$CLICKHOUSE_CLIENT --query "
    SELECT sumMerge(s_sum), minMerge(s_str), countMerge(s_cnt) FROM agg_roundtrip;
    SELECT sum(number) * 2, min(toString(number)), count() * 2 FROM numbers(10);
    TRUNCATE TABLE agg_roundtrip;
    INSERT INTO agg_roundtrip
    SELECT number % 97, sumState(toInt64(number)), minState(toString(number)), countState()
    FROM numbers(1000) GROUP BY number % 97;
    SELECT sumMerge(s_sum), minMerge(s_str), countMerge(s_cnt) FROM agg_roundtrip;
    SELECT sum(number), min(toString(number)), count() FROM numbers(1000);
    DROP TABLE agg_roundtrip;"
