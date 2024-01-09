#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    drop table if exists data;
    create table data engine=MergeTree() order by number as select * from numbers(1);
"
$CLICKHOUSE_CLIENT --optimize_trivial_count_query=0 --stage fetch_columns -q "select count() from data where indexHint(_partition_id = 'all')" --format CSVWithNames

$CLICKHOUSE_CLIENT -n -q "
    drop table if exists data;
    create table data engine=MergeTree() order by number as select * from numbers(0);
"
$CLICKHOUSE_CLIENT --optimize_trivial_count_query=0 --stage fetch_columns -q "select count() from data where indexHint(_partition_id = 'all')" --format CSVWithNames
