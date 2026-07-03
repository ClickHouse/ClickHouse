#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL "
create table test (d Nullable(UInt64)) engine=Memory;
create table test_buf  (d Nullable(UInt64)) engine=Buffer(default, test, 1, 10, 100, 10000, 1000000, 10000000, 100000000);
insert into test_buf select number from numbers(10);
insert into test_buf select NULL from numbers(10);
select d from test_buf order by all;
"
