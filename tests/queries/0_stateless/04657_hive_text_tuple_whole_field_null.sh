#!/usr/bin/env bash
# Tags: no-fasttest
# `HiveText` reuses the `CSV` field reader but keeps the comma as `tuple_delimiter` while its own
# field delimiter separates the columns, so a `Tuple` stays inside one field there. A null in that
# field is the whole column and must still give the column default, unlike `CSV` where the elements
# are separate columns.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="${CLICKHOUSE_TMP}/04657_hive_tuple_${CLICKHOUSE_TEST_UNIQUE_NAME}.txt"
printf '1\x01\\N\n' > "${DATA}"

$CLICKHOUSE_CLIENT -q "drop table if exists test_hive_tuple_null"
$CLICKHOUSE_CLIENT -q "create table test_hive_tuple_null (i Int8, t Tuple(String, Float64)) engine=MergeTree order by i"
$CLICKHOUSE_CLIENT -q "insert into test_hive_tuple_null from infile '${DATA}' FORMAT HIVETEXT"
$CLICKHOUSE_CLIENT -q "select * from test_hive_tuple_null"
$CLICKHOUSE_CLIENT -q "drop table test_hive_tuple_null"

rm -f "${DATA}"
