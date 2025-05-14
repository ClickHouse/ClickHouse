#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "1,2" > $CLICKHOUSE_TEST_UNIQUE_NAME.csv
$CLICKHOUSE_LOCAL -m -q "
create table test (x UInt64, y UInt32, size UInt64) engine=Memory;
insert into test select c1, c2, _size from file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv') settings use_structure_from_insertion_table_in_table_functions=1;
select * from test;
"
rm $CLICKHOUSE_TEST_UNIQUE_NAME.csv
