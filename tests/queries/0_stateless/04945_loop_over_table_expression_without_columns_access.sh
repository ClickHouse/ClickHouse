#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS loop_access_target;
    DROP TABLE IF EXISTS loop_access_alias;
    DROP TABLE IF EXISTS loop_access_live_target;
    DROP TABLE IF EXISTS loop_access_live_alias;

    CREATE TABLE loop_access_target (c0 Int32) ENGINE = Memory;
    CREATE TABLE loop_access_alias ENGINE = Alias(currentDatabase(), loop_access_target);
    DROP TABLE loop_access_target;

    CREATE TABLE loop_access_live_target (c0 Int32) ENGINE = Memory;
    INSERT INTO loop_access_live_target SELECT number FROM numbers(3);
    CREATE TABLE loop_access_live_alias ENGINE = Alias(currentDatabase(), loop_access_live_target);

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

echo "With SELECT on a live alias but not on its target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON loop_access_live_alias TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT 1 FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 2;" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With SELECT on a live alias and on its target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON loop_access_live_target TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT 1 FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 2;"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS loop_access_alias;
    DROP TABLE IF EXISTS loop_access_live_alias;
    DROP TABLE IF EXISTS loop_access_live_target;
"
