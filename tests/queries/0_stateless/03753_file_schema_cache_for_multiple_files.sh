#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select 'Hello' as c1, 42 as c2 format Parquet" > $CLICKHOUSE_TEST_UNIQUE_NAME.1.parquet
$CLICKHOUSE_LOCAL -q "select 42 as c1, 'Hello' as c2 format Parquet" > $CLICKHOUSE_TEST_UNIQUE_NAME.2.parquet
$CLICKHOUSE_LOCAL -m -q "
select sleepEachRow(2);
desc file('$CLICKHOUSE_TEST_UNIQUE_NAME.*.parquet') format Null;
select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.1.parquet');
select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.2.parquet');
"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.*
