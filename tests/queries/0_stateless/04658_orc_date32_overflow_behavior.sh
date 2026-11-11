#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ORC_FILE=${CLICKHOUSE_TMP}/04658_date32.orc

# The whole extended Date32 range [0000-01-01, 9999-12-31] round-trips through the ORC DATE type.
${CLICKHOUSE_LOCAL} -q "insert into function file('$ORC_FILE', ORC, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('0000-01-01'), ('1900-01-01'), ('2299-12-31'), ('9999-12-31')"
${CLICKHOUSE_LOCAL} -q "select * from file('$ORC_FILE', auto, 'date Date32')"

# A day number beyond the Date32 range: an error by default, the boundary value with 'saturate'.
# Date32 arithmetic does not clamp, so an out-of-range day number can be written to the file.
${CLICKHOUSE_LOCAL} -q "insert into function file('$ORC_FILE', ORC, 'date Date32') select toDate32('9999-12-31') + 100 settings engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} -q "select * from file('$ORC_FILE', auto, 'date Date32')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select * from file('$ORC_FILE', auto, 'date Date32') settings date_time_overflow_behavior='saturate'"

${CLICKHOUSE_LOCAL} -q "insert into function file('$ORC_FILE', ORC, 'date Date32') select toDate32('0000-01-01') - 100 settings engine_file_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} -q "select * from file('$ORC_FILE', auto, 'date Date32')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select * from file('$ORC_FILE', auto, 'date Date32') settings date_time_overflow_behavior='saturate'"

# An ORC DATE outside the Date range read with the Date type hint: an error by default, the boundary value with 'saturate'.
${CLICKHOUSE_LOCAL} -q "insert into function file('$ORC_FILE', ORC, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('9999-12-31'), ('0000-01-01')"
${CLICKHOUSE_LOCAL} -q "select * from file('$ORC_FILE', auto, 'date Date')" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"
${CLICKHOUSE_LOCAL} -q "select * from file('$ORC_FILE', auto, 'date Date') settings date_time_overflow_behavior='saturate'"

rm -f "$ORC_FILE"
