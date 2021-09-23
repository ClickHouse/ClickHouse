#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

uniq_num=$(date "+%Y%m%d%H%M%S%s")
table=tuple_filter_test_${uniq_num}

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${table}";


$CLICKHOUSE_CLIENT --query="CREATE TABLE ${table} (id UInt32, value String, log_date Date) Engine=MergeTree() ORDER BY id PARTITION BY log_date settings index_granularity=3;";


$CLICKHOUSE_CLIENT --query="insert into ${table} values (1, 'A','2021-01-01'),(2,'B','2021-01-01'),(3,'C','2021-01-01'),(4,'D','2021-01-02'),(5,'E','2021-01-02');";


$CLICKHOUSE_CLIENT --query="select * from ${table} where (id, value) = (1, 'A');";


$CLICKHOUSE_CLIENT --query="select * from ${table} where (log_date, value) = ('2021-01-01', 'A');";

#Make sure the quer log has been insert into query_log table
sleep 10 

$CLICKHOUSE_CLIENT --query="select read_rows from system.query_log where query in ('select * from ${table} where (id, value) = (1, \'A\');', 'select * from ${table} where (log_date, value) = (\'2021-01-01\', \'A\');') and type = 2;";

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${table}";

