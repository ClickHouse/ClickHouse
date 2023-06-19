#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

#Test file were created with:
#
#import pyarrow.parquet as pq
#import pyarrow as pa
#array = pa.array([5000, 20, 3000], type=pa.time32('s'))
#array64 = pa.array([5000000000, 20000000, 3000000000], type=pa.time64('us'))
#tab = pa.table({"timestamp": array})
#tab64 = pa.table({"timestamp": array64})
#pq.write_table(tab, "time32_test.parquet")
#pq.write_table(tab64, "time64_test.parquet")

#${CLICKHOUSE_CLIENT} --query "drop table dt_test"
${CLICKHOUSE_CLIENT} --query "create table dt_test (timestamp DateTime('UTC')) engine=Memory()"

cat ${CURDIR}/data_parquet/time32_test.parquet | ${CLICKHOUSE_CLIENT} --query "insert into dt_test FORMAT Parquet"

cat ${CURDIR}/data_parquet/time64_test.parquet | ${CLICKHOUSE_CLIENT} --query "insert into dt_test FORMAT Parquet"

${CLICKHOUSE_CLIENT} --query "select * from dt_test"