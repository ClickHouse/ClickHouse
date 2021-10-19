#!/usr/bin/env bash
# Tags: no-unbundled, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_orc/test_null_array.orc

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_null_array_orc"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_null_array_orc(col0 Array(Nullable(Int64)),col1 Array(Nullable(String))) ENGINE = Memory"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into test_null_array_orc format ORC"
${CLICKHOUSE_CLIENT} --query="select * from test_null_array_orc"

${CLICKHOUSE_CLIENT} --query="drop table test_null_array_orc"

#
#  test_null_array.orc is impossible to create using CH because it stores NULL instead of empty array
#  but CH able to ingest it as empty array []
#
#  import pyorc
#  with open("test_null_array.orc", "wb") as data:
#      with pyorc.Writer(data, "struct<col0:array<int>,col1:array<string>>") as writer:
#          writer.write(([0], ["Test 0"]))
#          writer.write(([None], [None]))
#          writer.write((None, None))
#
#
