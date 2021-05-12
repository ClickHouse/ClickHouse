#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_orc/nested_array_test.orc

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS orc_nested_arrays"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE orc_nested_arrays (a1 Array(Array(Array(UInt32))), a2 Array(Array(Array(String))), a3 Array(Array(Nullable(UInt32))), a4 Array(Array(Nullable(String)))) engine=Memory()"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into orc_nested_arrays format ORC"
timeout 3 ${CLICKHOUSE_CLIENT} -q "insert into orc_nested_arrays format ORC" < $DATA_FILE
${CLICKHOUSE_CLIENT} --query="select * from orc_nested_arrays"

${CLICKHOUSE_CLIENT} --query="drop table orc_nested_arrays"
