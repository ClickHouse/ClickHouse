#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the target of `remote`/`cluster` is a table function, there is no remote table, so the parsed
# remote table id stays at the meaningless parser default `system.one`. It must not take part in the
# access check of a local shard: `SELECT` on `system.one` is implicitly granted to everyone, but
# `INSERT` on it is not, so a restricted user used to get `ACCESS_DENIED` on `system.one` for
# `CREATE TABLE ... AS remote(..., table_function())` and for `INSERT INTO FUNCTION`, instead of the
# documented behavior (the read works, the insert is rejected with `NOT_IMPLEMENTED`).

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT CREATE TABLE, DROP TABLE, SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user};
    GRANT TABLE ENGINE ON Memory, TABLE ENGINE ON Distributed TO ${user};
    GRANT READ, WRITE ON REMOTE TO ${user};
"

CLIENT="${CLICKHOUSE_CLIENT} --user ${user}"

# Sanity checks: the user has `SELECT` on `system.one` implicitly, but no `INSERT` on it, and the
# named-table form of `remote` really does check access on the target table id.
${CLIENT} --query "SELECT count() FROM system.one"
${CLIENT} --query "INSERT INTO FUNCTION remote('127.0.0.1', system, one) VALUES (0)" 2>&1 | grep -oF 'ACCESS_DENIED' | head -n 1

${CLIENT} --query "SELECT count() FROM cluster('test_shard_localhost', numbers(10))"
${CLIENT} --query "SELECT count() FROM remote('127.0.0.1', numbers(10))"

${CLIENT} --query "INSERT INTO FUNCTION cluster('test_shard_localhost', numbers(10)) VALUES (100)" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -n 1
${CLIENT} --query "INSERT INTO FUNCTION remote('127.0.0.1', numbers(10)) VALUES (100)" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -n 1

${CLIENT} --query "
    CREATE TABLE table_over_remote_over_tf AS remote('127.0.0.1', numbers(10));
    SELECT count() FROM table_over_remote_over_tf;
"
${CLIENT} --query "INSERT INTO table_over_remote_over_tf VALUES (100)" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -n 1

# A named-table target still requires the corresponding privilege on that table.
${CLIENT} --query "
    CREATE TABLE src (x UInt8) ENGINE = Memory;
    INSERT INTO src VALUES (1);
    SELECT count() FROM remote('127.0.0.1', ${CLICKHOUSE_DATABASE}, src);
"

other="${CLICKHOUSE_DATABASE}_other"
${CLICKHOUSE_CLIENT} --query "
    DROP DATABASE IF EXISTS ${other};
    CREATE DATABASE ${other};
    CREATE TABLE ${other}.src (x UInt8) ENGINE = Memory;
"
${CLIENT} --query "SELECT count() FROM remote('127.0.0.1', ${other}, src)" 2>&1 | grep -oF 'ACCESS_DENIED' | head -n 1

${CLICKHOUSE_CLIENT} --query "
    DROP DATABASE ${other};
    DROP USER ${user};
"
