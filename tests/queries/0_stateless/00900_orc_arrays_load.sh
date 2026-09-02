#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS orc_arrays"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE orc_arrays (arr1 Array(Int8), arr2 Array(UInt8), arr3 Array(Int16), arr4 Array(UInt16), arr5 Array(Int32), arr6 Array(UInt32), arr7 Array(Int64), arr8 Array(UInt64), arr9 Array(String), arr10 Array(FixedString(4)), arr11 Array(Float32), arr12 Array(Float64), arr13 Array(Date), arr14 Array(Datetime), arr15 Array(Decimal(4, 2)), arr16 Array(Decimal(10, 2)), arr17 Array(Decimal(25, 2))) ENGINE=Memory()"

${CLICKHOUSE_CLIENT} --query="INSERT INTO orc_arrays VALUES ([1,-2,3],[1,2,3],[100,-200,300],[100,200,300],[10000000,-20000000,30000000],[10000000,2000000,3000000],[100000000000000,-200000000000,3000000000000],[100000000000000,20000000000000,3000000000000],['Some string','Some string','Some string'],['0000','1111','2222'],[42.42,424.2,0.4242],[424242.424242,4242042420.242424,42],['2000-01-01','2001-01-01','2002-01-01'],['2000-01-01 00:00:00','2001-01-01 00:00:00','2002-01-01 00:00:00'],[0.20,10.00,4.00],[4.00,10000.10,10000.10],[1000000000.00,90.00,101001.01]),([],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM orc_arrays FORMAT ORC" > "${CLICKHOUSE_TMP}"/arrays.orc

cat "${CLICKHOUSE_TMP}"/arrays.orc | ${CLICKHOUSE_CLIENT} -q "INSERT INTO orc_arrays FORMAT ORC"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM orc_arrays"
${CLICKHOUSE_CLIENT} --query="DROP TABLE orc_arrays"
