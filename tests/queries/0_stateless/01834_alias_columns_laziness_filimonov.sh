#!/usr/bin/env bash
# add_minmax_index_for_numeric_columns=0: Otherwise the insert needs to evaluate the function in the alias and requires
# sleeping for 100 rows * 0.1 seconds/row = 10 seconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
drop table if exists aliases_lazyness;
create table aliases_lazyness (x UInt32, y ALIAS sleepEachRow(0.1)) Engine=MergeTree ORDER BY x SETTINGS add_minmax_index_for_numeric_columns=0;
insert into aliases_lazyness(x) select * from numbers(100);
"

# In very old ClickHouse versions alias column was calculated for every row.
# If it works this way, the query will take at least 0.1 * 100 = 10 seconds.
# If the issue does not exist, the query should call sleepEachRow() only 1 time.
${CLICKHOUSE_CLIENT} --profile-events-delay-ms=-1 --print-profile-events --query "SELECT x, y FROM aliases_lazyness WHERE x = 1 FORMAT Null" |& grep -o -e "SleepFunctionMicroseconds.*" -e "SleepFunctionCalls.*"

${CLICKHOUSE_CLIENT} --query "drop table aliases_lazyness"
