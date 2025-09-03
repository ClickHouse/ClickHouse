#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "insert into function file('${CLICKHOUSE_TMP}/t0.parquet') select * from numbers(10)"
for i in {1..99}
do
    cp "${CLICKHOUSE_TMP}/t0.parquet" "${CLICKHOUSE_TMP}/t${i}.parquet"
done

${CLICKHOUSE_LOCAL} -q "select sum(number) from file('${CLICKHOUSE_TMP}/t{0..99}.parquet') settings input_format_parquet_preserve_order=1, input_format_parquet_use_native_reader_v3=1"

rm "${CLICKHOUSE_TMP}"/t{0..99}.parquet
