#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A LowCardinality(...) wrapper around a date/datetime target must not bypass the range
# validation of the format readers: they dispatch on the requested header type, and the final
# cast of the intermediate column to the header type is context-less, so a skipped check would
# silently clamp - or, for ORC, reinterpret the day count as unix seconds - regardless of
# `date_time_overflow_behavior`. `LowCardinality(DateTime64(...))` is not a valid type (the
# wrapper only accepts numbers, strings, Date and DateTime), so Date and DateTime cover all
# reachable wrapped date targets.

SUSPICIOUS="--allow_suspicious_low_cardinality_types=1"

for format in Parquet Arrow ArrowStream ORC Avro
do
    echo "$format"
    FILE=${CLICKHOUSE_TMP}/04849_date.$format

    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('9999-12-31'), ('0000-01-01')"

    # Out-of-range days must be rejected for LowCardinality targets exactly like for plain ones.
    ${CLICKHOUSE_LOCAL} $SUSPICIOUS -q "select * from file('$FILE', $format, 'date LowCardinality(DateTime(\'UTC\'))')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
    ${CLICKHOUSE_LOCAL} $SUSPICIOUS -q "select * from file('$FILE', $format, 'date LowCardinality(Nullable(DateTime(\'UTC\')))')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
    ${CLICKHOUSE_LOCAL} $SUSPICIOUS -q "select * from file('$FILE', $format, 'date LowCardinality(Date)')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
    ${CLICKHOUSE_LOCAL} $SUSPICIOUS -q "select * from file('$FILE', $format, 'date LowCardinality(DateTime(\'UTC\'))') settings date_time_overflow_behavior = 'saturate'"
    ${CLICKHOUSE_LOCAL} $SUSPICIOUS -q "select * from file('$FILE', $format, 'date LowCardinality(Date)') settings date_time_overflow_behavior = 'saturate'"

    # An in-range day count must be read as a calendar day (midnight of that day), not as seconds.
    ${CLICKHOUSE_LOCAL} -q "insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('1970-01-02')"
    ${CLICKHOUSE_LOCAL} $SUSPICIOUS -q "select * from file('$FILE', $format, 'date LowCardinality(DateTime(\'UTC\'))')"
    ${CLICKHOUSE_LOCAL} $SUSPICIOUS -q "select * from file('$FILE', $format, 'date LowCardinality(Date)')"

    rm -f "$FILE"
done
