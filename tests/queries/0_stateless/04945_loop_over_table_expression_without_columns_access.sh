#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS loop_access_target;
    DROP TABLE IF EXISTS loop_access_alias;

    CREATE TABLE loop_access_target (c0 Int32) ENGINE = Memory;
    CREATE TABLE loop_access_alias ENGINE = Alias(currentDatabase(), loop_access_target);
    DROP TABLE loop_access_target;

    CREATE USER ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${username};
"

echo "Without SELECT on the loop table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM loop(currentDatabase(), 'loop_access_alias');" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON loop_access_alias TO ${username};"

echo "With SELECT on the loop table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM loop(currentDatabase(), 'loop_access_alias');" 2>&1 |
    grep -o "UNSUPPORTED_METHOD" | uniq

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS loop_access_alias;
"
