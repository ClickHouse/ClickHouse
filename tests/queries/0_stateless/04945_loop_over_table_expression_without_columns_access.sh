#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_col="${username}_col"
user_show="${username}_show"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP USER IF EXISTS ${user_col};
    DROP USER IF EXISTS ${user_show};
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

    CREATE USER ${user_col} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user_col};
    GRANT SELECT ON loop_access_live_alias TO ${user_col};
    GRANT SELECT(c0) ON loop_access_live_target TO ${user_col};

    CREATE USER ${user_show} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user_show};
    GRANT SELECT ON loop_access_live_alias TO ${user_show};
    GRANT SHOW COLUMNS ON loop_access_live_target TO ${user_show};
"

echo "Without SELECT on the loop table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM loop(currentDatabase(), 'loop_access_alias');" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON loop_access_alias TO ${username};"

echo "With SELECT on the loop table"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM loop(currentDatabase(), 'loop_access_alias');" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "Describing the dangling alias directly"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE TABLE loop_access_alias;" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With SELECT on the dropped target name"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON loop_access_target TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT count() FROM loop(currentDatabase(), 'loop_access_alias');" 2>&1 |
    grep -o "UNSUPPORTED_METHOD" | uniq

echo "With SELECT on a live alias but not on its target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON loop_access_live_alias TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT 1 FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 2;" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "Reading no rows through loop without a grant on the target"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT * FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 0 FORMAT TSVWithNames;" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "Describing a row-less loop subquery without a grant on the target"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "DESCRIBE TABLE (SELECT * FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 0);" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "Reading no rows from the live alias directly"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT * FROM loop_access_live_alias LIMIT 0 FORMAT TSVWithNames;" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With only a column grant on the target, through loop"
${CLICKHOUSE_CLIENT} --user="${user_col}" --query \
    "SELECT c0 FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 0 FORMAT TSVWithNames;" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With only a column grant on the target, reading the alias directly"
${CLICKHOUSE_CLIENT} --user="${user_col}" --query \
    "SELECT c0 FROM loop_access_live_alias LIMIT 0 FORMAT TSVWithNames;"

echo "With only SHOW COLUMNS on the target, through loop"
${CLICKHOUSE_CLIENT} --user="${user_show}" --query \
    "SELECT * FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 0 FORMAT TSVWithNames;" 2>&1 |
    grep -o "ACCESS_DENIED" | uniq

echo "With SELECT on a live alias and on its target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON loop_access_live_target TO ${username};"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT 1 FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 2;"

echo "Reading no rows through loop with SELECT on the target"
${CLICKHOUSE_CLIENT} --user="${username}" --query \
    "SELECT * FROM loop(currentDatabase(), 'loop_access_live_alias') LIMIT 0 FORMAT TSVWithNames;"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP USER IF EXISTS ${user_col};
    DROP USER IF EXISTS ${user_show};
    DROP TABLE IF EXISTS loop_access_alias;
    DROP TABLE IF EXISTS loop_access_live_alias;
    DROP TABLE IF EXISTS loop_access_live_target;
"
