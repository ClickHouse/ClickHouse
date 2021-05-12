#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_orc/array_test.orc

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS orc_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE orc_arrays (a1 Array(Int8), a2 Array(UInt8), a3 Array(Int16), a4 Array(UInt16), a5 Array(Int32), a6 Array(UInt32), a7 Array(Int64), a8 Array(UInt64), a9 Array(String), a10 Array(FixedString(4)), a11 Array(Float32), a12 Array(Float64), a13 Array(Date), a14 Array(Datetime), a15 Array(Decimal(4, 2)), a16 Array(Decimal(10, 2)), a17 Array(Decimal(25, 2))) ENGINE=Memory()"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into orc_arrays format ORC"
timeout 3 ${CLICKHOUSE_CLIENT} -q "insert into orc_arrays format ORC" < $DATA_FILE
${CLICKHOUSE_CLIENT} --query="select * from orc_arrays"

${CLICKHOUSE_CLIENT} --query="drop table orc_arrays"
