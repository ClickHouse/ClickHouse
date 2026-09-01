#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AVRO_FILE=${CLICKHOUSE_TMP}/04670_date32.avro

# The whole extended Date32 range [0000-01-01, 9999-12-31] round-trips through the Avro `date` logical type.
${CLICKHOUSE_LOCAL} -q "insert into function file('$AVRO_FILE', Avro, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('0000-01-01'), ('1900-01-01'), ('2299-12-31'), ('9999-12-31')"
${CLICKHOUSE_LOCAL} -q "desc file('$AVRO_FILE')"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE')"

# A day number beyond the Date32 range: an error by default, the boundary value with 'saturate'.
# Date32 arithmetic does not clamp, so an out-of-range day number can be written to the file.
${CLICKHOUSE_LOCAL} -q "insert into function file('$AVRO_FILE', Avro, 'date Date32') select toDate32('9999-12-31') + 100 settings engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE') settings date_time_overflow_behavior='saturate'"

${CLICKHOUSE_LOCAL} -q "insert into function file('$AVRO_FILE', Avro, 'date Date32') select toDate32('0000-01-01') - 100 settings engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE') settings date_time_overflow_behavior='saturate'"

# A plain Avro `int` read with the Date32 type hint is validated the same way.
${CLICKHOUSE_LOCAL} -q "insert into function file('$AVRO_FILE', Avro, 'date Int32') settings engine_file_truncate_on_insert = 1 values (3000000)"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE', auto, 'date Date32')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select * from file('$AVRO_FILE', auto, 'date Date32') settings date_time_overflow_behavior='saturate'"

rm -f "$AVRO_FILE"
