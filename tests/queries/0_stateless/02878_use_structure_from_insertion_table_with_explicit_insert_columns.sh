#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select 42 as x format Native" > $CLICKHOUSE_TEST_UNIQUE_NAME.native
$CLICKHOUSE_LOCAL -q "
create table test (x UInt64, y UInt64) engine=Memory;
insert into test (x) select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.native');
insert into test (y) select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.native');
insert into test (* except(x)) select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.native');
insert into test (* except(y)) select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.native');
select * from test order by x;
"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.native
