#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

FILE_NAME=conversion_to_datetime64_test.parquet
cp "$CURDIR/data_parquet/$FILE_NAME" "$USER_FILES_PATH/$FILE_NAME"

# Test file were created with:
#
# import pyarrow.parquet as pq
# import pyarrow as pa
# id = pa.array(["1"], type=pa.string())
# ts = pa.array(["2020-01-01 14:00:00"], type=pa.string())
# ts_plus_tz = pa.array(["2020-01-01 14:00:00+00:00"], type=pa.string())
# tab = pa.table({"id": id, "ts": ts, "ts_plus_tz": ts_plus_tz})
# pq.write_table(tab, "conversion_to_datetime64_test.parquet")

echo 'FROM ts:'

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (ts DateTime64(9, 'UTC')) ENGINE=MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test (ts) SELECT ts FROM file('conversion_to_datetime64_test.parquet')"
${CLICKHOUSE_CLIENT} --query "SELECT * from test"
echo '---'
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test (ts) SELECT ts FROM file('conversion_to_datetime64_test.par?uet')"
${CLICKHOUSE_CLIENT} --query "SELECT * from test"
echo '---'
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test (ts) SELECT ts FROM file('{conversion_to_datetime64_test.parquet, conversion_to_datetime64_test.parquet}')"
${CLICKHOUSE_CLIENT} --query "SELECT * from test"

echo -e '\nFROM ts_plus_tz:'

${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test (ts) SELECT ts_plus_tz FROM file('conversion_to_datetime64_test.parquet')" 2>&1 | grep -q "Cannot parse string '2020-01-01 14:00:00+00:00' as DateTime64(9, 'UTC')" && echo "CANNOT_PARSE" || echo "FAIL"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test (ts) SELECT ts_plus_tz FROM file('conversion_to_datetime64_test.par?uet')" 2>&1 | grep -q "Cannot parse string '2020-01-01 14:00:00+00:00' as DateTime64(9, 'UTC')" && echo "CANNOT_PARSE" || echo "FAIL"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test (ts) SELECT ts_plus_tz FROM file('{conversion_to_datetime64_test.parquet, conversion_to_datetime64_test.parquet}')" 2>&1 | grep -q "Cannot parse string '2020-01-01 14:00:00+00:00' as DateTime64(9, 'UTC')" && echo "CANNOT_PARSE" || echo "FAIL"
${CLICKHOUSE_CLIENT} --query "SELECT * from test"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
