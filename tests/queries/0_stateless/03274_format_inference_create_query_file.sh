#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select 42 as a format JSONEachRow" > data_$CLICKHOUSE_TEST_UNIQUE_NAME
$CLICKHOUSE_LOCAL -nm -q "
create table test engine=File(auto, './data_$CLICKHOUSE_TEST_UNIQUE_NAME');
show create table test;
drop table test;

create table test engine=File(auto, './data_$CLICKHOUSE_TEST_UNIQUE_NAME', 'none');
show create table test;
drop table test;
" | grep "JSON" -c

rm data_$CLICKHOUSE_TEST_UNIQUE_NAME

