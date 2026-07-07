#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 9-byte decimals produced by spark in integration test test_storage_delta/test.py::test_single_log_file

${CLICKHOUSE_CLIENT} --query="drop table if exists 02845_parquet_odd_decimals"
${CLICKHOUSE_CLIENT} --query="create table 02845_parquet_odd_decimals (\`col-1de12c05-5dd5-4fa7-9f93-33c43c9a4028\` Decimal(20, 0), \`col-5e1b97f1-dade-4c7d-b71b-e31d789e01a4\` String) engine Memory"
${CLICKHOUSE_CLIENT} --query="insert into 02845_parquet_odd_decimals from infile '$CUR_DIR/data_parquet/nine_byte_decimals_from_spark.parquet'"
${CLICKHOUSE_CLIENT} --query="select count() from 02845_parquet_odd_decimals"
