#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="
drop table if exists test;
create table test (c0 Int, c1 DateTime) engine=MergeTree order by tuple() ttl c1 SETTINGS ratio_of_defaults_for_sparse_serialization = 0.001;
insert into test (c0) select * from numbers(10);
insert into function file(${CLICKHOUSE_TEST_UNIQUE_NAME}.csv) select * from test;
insert into test select * from file(${CLICKHOUSE_TEST_UNIQUE_NAME}.csv);
drop table test;
"