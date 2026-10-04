#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `DROP TABLE ... IF EMPTY` checks the privilege to drop before it looks at the table: otherwise a
# user who may not drop a table could tell an empty one (`ACCESS_DENIED`) from a non-empty one
# (`TABLE_NOT_EMPTY`). Once the user may drop it, a row policy that hides every row from them
# does not make a non-empty table look empty.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user};
    CREATE TABLE t_full (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_empty (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_full VALUES (1);
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
"

for table in t_full t_empty
do
    ${CLICKHOUSE_CLIENT} --user "${user}" \
        --query "DROP TABLE IF EMPTY ${CLICKHOUSE_DATABASE}.${table} SETTINGS ignore_drop_queries_probability = 0" 2>&1 \
        | grep -o -m1 -E 'ACCESS_DENIED|TABLE_NOT_EMPTY'
done

${CLICKHOUSE_CLIENT} --query "
    GRANT DROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${user};
    CREATE ROW POLICY policy_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_full USING 0 TO ${user};
"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_full"

${CLICKHOUSE_CLIENT} --user "${user}" \
    --query "DROP TABLE IF EMPTY ${CLICKHOUSE_DATABASE}.t_full SETTINGS ignore_drop_queries_probability = 0" 2>&1 \
    | grep -o -m1 -E 'ACCESS_DENIED|TABLE_NOT_EMPTY'
${CLICKHOUSE_CLIENT} --user "${user}" \
    --query "DROP TABLE IF EMPTY ${CLICKHOUSE_DATABASE}.t_empty SETTINGS ignore_drop_queries_probability = 0"

${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_%' ORDER BY name;
    DROP ROW POLICY policy_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_full;
    DROP TABLE t_full;
    DROP USER ${user};
"
