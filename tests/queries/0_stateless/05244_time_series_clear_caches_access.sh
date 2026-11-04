#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The name is unique per run, so the test can run in parallel with itself.
user="user_05244_$CLICKHOUSE_DATABASE"
cluster="test_shard_localhost"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 --query "CREATE TABLE ts ENGINE = TimeSeries"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE not_time_series (x UInt8) ENGINE = Memory"

# The user can run queries on the cluster, but has no privilege on the caches.
${CLICKHOUSE_CLIENT} --query "CREATE USER $user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $user"

run() { ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --user "$user" --query "$1"; }

echo "--- without the grant the statement is denied before the table is looked up ---"
run "SYSTEM CLEAR TIME SERIES CACHES ts -- { serverError ACCESS_DENIED }"
run "SYSTEM CLEAR TIME SERIES CACHES unknown_table -- { serverError ACCESS_DENIED }"
run "SYSTEM CLEAR TIME SERIES CACHES not_time_series -- { serverError ACCESS_DENIED }"
run "SYSTEM CLEAR TIME SERIES CACHES ON CLUSTER $cluster $CLICKHOUSE_DATABASE.ts -- { serverError ACCESS_DENIED }"

echo "--- with the grant the statement runs, and the errors about the table appear ---"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM CLEAR TIME SERIES CACHES ON *.* TO $user"
run "SYSTEM CLEAR TIME SERIES CACHES ts"
run "SYSTEM CLEAR TIME SERIES CACHES ON CLUSTER $cluster $CLICKHOUSE_DATABASE.ts"
run "SYSTEM CLEAR TIME SERIES CACHES unknown_table -- { serverError UNKNOWN_TABLE }"
run "SYSTEM CLEAR TIME SERIES CACHES not_time_series -- { serverError UNEXPECTED_TABLE_ENGINE }"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ts, not_time_series"
