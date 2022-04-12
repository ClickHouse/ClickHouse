#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --query "
drop table if exists aliases_lazyness;
create table aliases_lazyness (x UInt32, y ALIAS sleepEachRow(0.1)) Engine=MergeTree ORDER BY x;
insert into aliases_lazyness(x) select * from numbers(40);
"

# In very old ClickHouse versions alias column was calculated for every row.
# If it works this way, the query will take at least 0.1 * 40 = 4 seconds.
# If the issue does not exist, the query should take slightly more than 0.1 seconds.
# The exact time is not guaranteed, so we check in a loop that at least once 
# the query will process in less than one second, that proves that the behaviour is not like it was long time ago.

i=0 retries=300
while [[ $i -lt $retries ]]; do
    timeout 1 ${CLICKHOUSE_CLIENT} --query "SELECT x, y FROM aliases_lazyness WHERE x = 1 FORMAT Null" && break
    ((++i))
done

${CLICKHOUSE_CLIENT} --multiquery --query "
drop table aliases_lazyness;
SELECT 'Ok';
"
