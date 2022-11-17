#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Parquet"

DATA_FILE=$CUR_DIR/data_parquet/int-list-zero-based-chunked-array.parquet
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_load (arr Array(Int64)) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO parquet_load FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_load"
${CLICKHOUSE_CLIENT} --query="drop table parquet_load"