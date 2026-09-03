#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for format in Parquet Arrow ArrowStream
do
    echo "$format"
    FILE=${CLICKHOUSE_TMP}/04761_date.$format

    # The whole Date range [1970-01-01, 2149-06-06] round-trips through the date types
    # of these formats when read with a Date type hint.
    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date') settings engine_file_truncate_on_insert = 1 values ('1970-01-01'), ('2149-06-06')"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date Date')"

    # A date beyond the Date range must not wrap in the UInt16 representation:
    # an error by default, the boundary value with 'saturate'.
    # 9999-12-31 is day 2932896, which without validation would wrap to day 49312 = 2105-01-05,
    # and 0000-01-01 is day -719528, which would wrap to day 1368 = 1973-09-30.
    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('9999-12-31'), ('0000-01-01')"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date Date')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date Date') settings date_time_overflow_behavior='saturate'"

    # A day number whose midnight does not fit into DateTime must not wrap through the
    # context-less Date32 -> DateTime cast: 9999-12-31 is day 2932896, which without validation
    # would wrap to an unrelated timestamp in 2106, and 0000-01-01 would wrap to one in 2033.
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime(\'UTC\')')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime(\'UTC\')') settings date_time_overflow_behavior='saturate'"

    # The DateTime-representable day range [1970-01-01, 2106-02-06] round-trips with a DateTime type hint.
    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('1970-01-01'), ('2106-02-06')"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime(\'UTC\')')"

    rm -f "$FILE"
done
