#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

caller_database="execute_as_caller_${CLICKHOUSE_DATABASE}"
target_database="execute_as_target_${CLICKHOUSE_DATABASE}"
caller="execute_as_caller_${CLICKHOUSE_DATABASE}"
target="execute_as_target_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${caller}, ${target}"
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${caller_database}"
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${target_database}"
}

cleanup

$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${caller_database}"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${target_database}"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${caller_database}.t (value String) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${target_database}.t (value String) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${caller_database}.t VALUES ('caller')"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${target_database}.t VALUES ('target')"

$CLICKHOUSE_CLIENT --query "CREATE USER ${caller} IDENTIFIED WITH no_password DEFAULT DATABASE ${caller_database}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${target} IDENTIFIED WITH no_password DEFAULT DATABASE ${target_database}"
$CLICKHOUSE_CLIENT --query "GRANT IMPERSONATE ON ${target} TO ${caller}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${caller_database}.t TO ${target}"

$CLICKHOUSE_CLIENT --user "${caller}" --query "EXECUTE AS ${target} SELECT value FROM t"

cleanup
