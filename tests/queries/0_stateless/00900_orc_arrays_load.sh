#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_orc/array_test.orc

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS orc_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE orc_arrays (arr1 Array(Int8), arr2 Array(UInt8), arr3 Array(Int16), arr4 Array(UInt16), arr5 Array(Int32), arr6 Array(UInt32), arr7 Array(Int64), arr8 Array(UInt64), arr9 Array(String), arr10 Array(FixedString(4)), arr11 Array(Float32), arr12 Array(Float64), arr13 Array(Date), arr14 Array(Datetime), arr15 Array(Decimal(4, 2)), arr16 Array(Decimal(10, 2)), arr17 Array(Decimal(25, 2))) ENGINE=Memory()"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into orc_arrays format ORC"
${CLICKHOUSE_CLIENT} --query="select * from orc_arrays"

${CLICKHOUSE_CLIENT} --query="drop table orc_arrays"
