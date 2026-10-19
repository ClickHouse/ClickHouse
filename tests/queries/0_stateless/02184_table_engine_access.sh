#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user};"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY '${user}';"
${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM ${user}"

$CLICKHOUSE_CLIENT --query "GRANT CREATE ON *.* TO ${user};"
$CLICKHOUSE_CLIENT --query "GRANT SHOW COLUMNS ON *.* TO ${user};"

$CLICKHOUSE_CLIENT --query "CREATE TABLE url ENGINE=URL('https://clickhouse.com', LineAsString)"

$CLICKHOUSE_CLIENT  --user="${user}" --password="${user}"  --query "CREATE TABLE t AS url" 2>&1| grep -Fo "ACCESS_DENIED" | uniq

$CLICKHOUSE_CLIENT --query "GRANT READ, WRITE ON URL TO ${user};"
$CLICKHOUSE_CLIENT --user="${user}" --password="${user}"  --query "CREATE TABLE t AS url"
$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE t"
$CLICKHOUSE_CLIENT --query "DROP TABLE t"
$CLICKHOUSE_CLIENT --query "DROP TABLE url"
$CLICKHOUSE_CLIENT --query "DROP USER ${user}"
