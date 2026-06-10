#!/usr/bin/env bash
# Tags: no-replicated-database

# Verify that DESCRIBE TABLE and CREATE TABLE AS on table functions enforce
# source access checks. Previously these paths bypassed RBAC — a user without
# e.g. READ ON URL could still get schema info or trigger outbound connections.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_04028_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TEMPORARY TABLE ON *.* TO $user"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${db}.* TO $user"

echo "--- DESCRIBE url() without READ ON URL ---"
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE TABLE url('http://example.com/data.csv', 'CSV', 'x UInt32, secret String')" 2>&1 | grep -m1 -o "ACCESS_DENIED"

echo "--- DESCRIBE file() without READ ON FILE ---"
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE TABLE file('nonexistent.csv', 'CSV', 'x UInt32, secret String')" 2>&1 | grep -m1 -o "ACCESS_DENIED"

echo "--- CREATE TABLE AS url() without READ ON URL ---"
${CLICKHOUSE_CLIENT} --user $user --query "CREATE TABLE ${db}.t_04028 AS url('http://example.com/data.csv', 'CSV', 'x UInt32, secret String')" 2>&1 | grep -m1 -o "ACCESS_DENIED"

echo "--- Grant READ ON URL ---"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL TO $user"
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE TABLE url('http://example.com/data.csv', 'CSV', 'x UInt32, secret String')" | cut -f1

echo "--- Grant READ ON FILE ---"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON FILE TO $user"
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE TABLE file('nonexistent.csv', 'CSV', 'x UInt32, secret String')" | cut -f1

echo "--- Cleanup ---"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.t_04028"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
