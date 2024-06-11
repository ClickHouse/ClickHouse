#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "1,2" > $CLICKHOUSE_TEST_UNIQUE_NAME.csv
sleep 1
$CLICKHOUSE_LOCAL -nm -q "
create table test (x UInt64, y UInt32, size UInt64, d32 DateTime32, d64 DateTime64) engine=Memory;
insert into test select c1, c2, _size, _time, _time from file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv') settings use_structure_from_insertion_table_in_table_functions=1;
select x, y, size, (dateDiff('millisecond', d32, now()) < 4000 AND dateDiff('millisecond', d32, now()) > 0), (dateDiff('second', d64, now()) < 4 AND dateDiff('second', d64, now()) > 0)  from test;
"
rm $CLICKHOUSE_TEST_UNIQUE_NAME.csv
