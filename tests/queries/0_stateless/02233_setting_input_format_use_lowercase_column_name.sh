#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Parquet"
DATA_FILE=$CUR_DIR/02233_data/test_setting_input_format_use_lowercase_column_name.parquet
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_load (id String, score Int32) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO parquet_load FORMAT Parquet SETTINGS input_format_use_lowercase_column_name=true"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_load"
${CLICKHOUSE_CLIENT} --query="drop table parquet_load"

echo "ORC"
DATA_FILE=$CUR_DIR/02233_data/test_setting_input_format_use_lowercase_column_name.orc
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS orc_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE orc_load (id String, score Int32) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO orc_load FORMAT ORC SETTINGS input_format_use_lowercase_column_name=true"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM orc_load"
${CLICKHOUSE_CLIENT} --query="drop table orc_load"
