#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# that test is failing on versions <= 19.11.12

${CLICKHOUSE_CLIENT} --multiquery --query="
    DROP TABLE IF EXISTS lc_empty_part_bug;
    create table lc_empty_part_bug (id  UInt64, s String) Engine=MergeTree ORDER BY id;
    insert into lc_empty_part_bug select number as id, toString(rand()) from numbers(100);
    alter table lc_empty_part_bug delete where id < 100;
" --mutations_sync=1

echo 'Waited for mutation to finish'

${CLICKHOUSE_CLIENT} --multiquery --query="
    alter table lc_empty_part_bug modify column s LowCardinality(String);
    SELECT 'still alive';
    insert into lc_empty_part_bug select number+100 as id, toString(rand()) from numbers(100);
    SELECT count() FROM lc_empty_part_bug WHERE not ignore(*);
    DROP TABLE lc_empty_part_bug;
" --mutations_sync=1
