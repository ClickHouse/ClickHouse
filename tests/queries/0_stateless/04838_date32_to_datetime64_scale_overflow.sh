#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `DateTime64` ticks are stored in an Int64, so the representable range shrinks as the scale grows:
# at scale 9 it ends at 2262-04-11 and starts at 1677-09-21, far inside the `Date32` range.
# A `Date32` value outside that window must not silently come back as the clamped boundary when
# `date_time_overflow_behavior = 'throw'`.

echo "cast"
${CLICKHOUSE_LOCAL} -q "select cast(toDate32('9999-12-31') as DateTime64(9, 'UTC')) settings date_time_overflow_behavior = 'throw'" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select cast(toDate32('0000-01-01') as DateTime64(9, 'UTC')) settings date_time_overflow_behavior = 'throw'" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select cast(toDate32('9999-12-31') as DateTime64(9, 'UTC')), cast(toDate32('0000-01-01') as DateTime64(9, 'UTC')) settings date_time_overflow_behavior = 'saturate'"
# The default ('ignore') keeps clamping rather than failing with `DECIMAL_OVERFLOW`.
${CLICKHOUSE_LOCAL} -q "select cast(toDate32('9999-12-31') as DateTime64(9, 'UTC')), cast(toDate32('0000-01-01') as DateTime64(9, 'UTC'))"
# The scale-9 boundary days themselves are exact, and a lower scale covers the whole `Date32` range.
${CLICKHOUSE_LOCAL} -q "select cast(toDate32('2262-04-11') as DateTime64(9, 'UTC')), cast(toDate32('1677-09-22') as DateTime64(9, 'UTC')) settings date_time_overflow_behavior = 'throw'"
${CLICKHOUSE_LOCAL} -q "select cast(toDate32('9999-12-31') as DateTime64(3, 'UTC')), cast(toDate32('0000-01-01') as DateTime64(3, 'UTC')) settings date_time_overflow_behavior = 'throw'"

for format in Parquet Arrow ArrowStream ORC Avro
do
    echo "$format"
    FILE=${CLICKHOUSE_TMP}/04838_date.$format

    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('9999-12-31'), ('0000-01-01')"

    # A day whose midnight is not representable at scale 9 must be rejected, not clamped.
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime64(9, \'UTC\')')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime64(9, \'UTC\')') settings date_time_overflow_behavior = 'saturate'"

    # Scale 3 represents the whole `Date32` range, so nothing is rejected there.
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime64(3, \'UTC\')')"

    # The scale-9 boundary days round-trip exactly.
    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('1677-09-22'), ('2262-04-11')"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime64(9, \'UTC\')')"

    rm -f "$FILE"
done
