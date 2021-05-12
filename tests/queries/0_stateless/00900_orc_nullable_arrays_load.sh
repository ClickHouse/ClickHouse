#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_orc/nullable_array_test.orc

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS orc_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE orc_load (a1 Array(Nullable(UInt32)), a2 Array(Nullable(String)), a3 Array(Nullable(Decimal(4, 2)))) ENGINE=Memory()"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into orc_load format ORC"
timeout 3 ${CLICKHOUSE_CLIENT} -q "insert into orc_load format ORC" < $DATA_FILE
${CLICKHOUSE_CLIENT} --query="select * from orc_load"

${CLICKHOUSE_CLIENT} --query="drop table orc_load"
