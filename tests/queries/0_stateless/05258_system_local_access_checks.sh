#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Local (not ON CLUSTER) access checks of SYSTEM commands:
# - SYSTEM SYNC TRANSACTION LOG requires the global SYSTEM SYNC TRANSACTION LOG grant.
# - SYSTEM SYNC DATABASE REPLICA requires SYSTEM SYNC DATABASE REPLICA on the database.
# - SYSTEM START/STOP VIRTUAL PARTS UPDATE db.tbl accepts a table-level grant.

table="t_05258"
other_table="t_05258_other"
user="user_05258_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table, $other_table"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $table (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $other_table (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user IDENTIFIED WITH no_password"

echo "no grants"
${CLICKHOUSE_CLIENT} --user "$user" --query "SYSTEM SYNC TRANSACTION LOG -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user "$user" --query "SYSTEM SYNC DATABASE REPLICA $CLICKHOUSE_DATABASE -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user "$user" --query "SYSTEM STOP VIRTUAL PARTS UPDATE $CLICKHOUSE_DATABASE.$table -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM SYNC DATABASE REPLICA ON $CLICKHOUSE_DATABASE.* TO $user"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM VIRTUAL PARTS UPDATE ON $CLICKHOUSE_DATABASE.$table TO $user"

echo "with grants"
# The database is not Replicated, so the command passes the access check and then fails.
${CLICKHOUSE_CLIENT} --user "$user" --query "SYSTEM SYNC DATABASE REPLICA $CLICKHOUSE_DATABASE -- { serverError BAD_ARGUMENTS }"
# The table-level grant is enough. The command itself is not implemented in the open-source build.
is_cloud=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
out=$(${CLICKHOUSE_CLIENT} --user "$user" --query "SYSTEM STOP VIRTUAL PARTS UPDATE $CLICKHOUSE_DATABASE.$table" 2>&1)
if grep -qF ACCESS_DENIED <<< "$out"; then
    echo "access denied: $out"
elif [ "$is_cloud" = 1 ] || grep -qF SUPPORT_IS_DISABLED <<< "$out"; then
    echo "ok"
else
    echo "expected SUPPORT_IS_DISABLED: $out"
fi
# A grant on another table does not help.
${CLICKHOUSE_CLIENT} --user "$user" --query "SYSTEM STOP VIRTUAL PARTS UPDATE $CLICKHOUSE_DATABASE.$other_table -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $table, $other_table"
