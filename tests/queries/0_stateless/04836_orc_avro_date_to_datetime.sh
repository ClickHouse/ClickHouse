#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for format in ORC Avro
do
    echo "$format"
    FILE=${CLICKHOUSE_TMP}/04836_date.$format

    # A date read into DateTime / DateTime64 is a day count, not unix seconds or raw ticks:
    # day 1 must become midnight of 1970-01-02, not 1970-01-01 00:00:01 (or .000000001).
    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('1970-01-02'), ('2106-02-06')"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime(\'UTC\')')"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime64(3, \'UTC\')')"

    # A day number whose midnight does not fit into DateTime must not wrap:
    # an error by default, the boundary value with 'saturate'.
    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('9999-12-31'), ('0000-01-01')"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime(\'UTC\')')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime(\'UTC\')') settings date_time_overflow_behavior='saturate'"

    # DateTime64 covers the whole Date32 range at this scale.
    ${CLICKHOUSE_LOCAL} -q "select * from file('$FILE', $format, 'date DateTime64(3, \'UTC\')')"

    rm -f "$FILE"
done
