#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AVRO_FILE=${CLICKHOUSE_TMP}/04759_date.avro

# The whole Date range [1970-01-01, 2149-06-06] round-trips through the Avro `date` logical type
# when read with a Date type hint.
${CLICKHOUSE_LOCAL} -q "insert into function file('$AVRO_FILE', Avro, 'date Date') settings engine_file_truncate_on_insert = 1 values ('1970-01-01'), ('2149-06-06')"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE', auto, 'date Date')"

# An Avro `date` beyond the Date range must not wrap in the UInt16 representation:
# an error by default, the boundary value with 'saturate'.
# 9999-12-31 is day 2932896, which without validation would wrap to day 49312 = 2105-01-05.
${CLICKHOUSE_LOCAL} -q "insert into function file('$AVRO_FILE', Avro, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('9999-12-31'), ('0000-01-01')"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE', auto, 'date Date')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE', auto, 'date Date') settings date_time_overflow_behavior='saturate'"

# A plain Avro `int` read with the Date type hint is validated the same way.
${CLICKHOUSE_LOCAL} -q "insert into function file('$AVRO_FILE', Avro, 'date Int32') settings engine_file_truncate_on_insert = 1 values (65536), (-1)"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE', auto, 'date Date')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE', auto, 'date Date') settings date_time_overflow_behavior='saturate'"

rm -f "$AVRO_FILE"
