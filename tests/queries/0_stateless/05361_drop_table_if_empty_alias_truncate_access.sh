#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `TRUNCATE TABLE ... IF EMPTY` of an `Alias` truncates its target, so it checks the privilege to
# truncate the target before it looks at the target's row count: otherwise a user who may truncate
# the alias but not the target could tell an empty target (`ACCESS_DENIED`) from a non-empty one
# (`TABLE_NOT_EMPTY`).

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user};
    CREATE TABLE t_full (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_empty (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_full VALUES (1);
    CREATE TABLE a_full ENGINE = Alias('t_full');
    CREATE TABLE a_empty ENGINE = Alias('t_empty');
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SHOW TABLES ON ${db}.* TO ${user};
    GRANT TRUNCATE ON ${db}.a_full TO ${user};
    GRANT TRUNCATE ON ${db}.a_empty TO ${user};
"

for table in a_full a_empty
do
    ${CLICKHOUSE_CLIENT} --user "${user}" --query "TRUNCATE TABLE IF EMPTY ${db}.${table}" 2>&1 \
        | grep -o -m1 -E 'ACCESS_DENIED|TABLE_NOT_EMPTY'
done

${CLICKHOUSE_CLIENT} --query "
    GRANT TRUNCATE ON ${db}.t_full TO ${user};
    GRANT TRUNCATE ON ${db}.t_empty TO ${user};
"

${CLICKHOUSE_CLIENT} --user "${user}" --query "TRUNCATE TABLE IF EMPTY ${db}.a_full" 2>&1 \
    | grep -o -m1 -E 'ACCESS_DENIED|TABLE_NOT_EMPTY'
${CLICKHOUSE_CLIENT} --user "${user}" --query "TRUNCATE TABLE IF EMPTY ${db}.a_empty"

${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM t_full;
    DROP USER ${user};
"
