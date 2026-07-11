#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest, no-tsan, no-msan, no-asan
# It is too slow under TSan
# It eats too much memory under ASan or MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Parquet"

DATA_FILE=$CUR_DIR/data_parquet/list_monotonically_increasing_offsets.parquet
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_load (list Array(Int64), json Nullable(String)) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} --max_memory_usage 10G -q "INSERT INTO parquet_load FORMAT Parquet"
${CLICKHOUSE_CLIENT} --max_result_rows 0 --max_result_bytes 0 --query="SELECT * FROM parquet_load" | md5sum
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM parquet_load"
${CLICKHOUSE_CLIENT} --query="drop table parquet_load"
