#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -nm -q "
create table test (s String) engine=MergeTree order by tuple();
insert into test select '{\"a\" : 42}' from numbers(10);
alter table test add column json JSON materialized s::JSON;
select s, json from test limit 10;
drop table test;
"
