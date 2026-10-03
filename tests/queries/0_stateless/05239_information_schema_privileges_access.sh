#!/usr/bin/env bash
# INFORMATION_SCHEMA *_PRIVILEGES views are SQL SECURITY INVOKER over system.grants:
# without the right to read system.grants the user gets ACCESS_DENIED, not an empty result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05239_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} -q "CREATE USER $user"

echo "-- no grants: denied"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT * FROM information_schema.user_privileges" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "-- SELECT on information_schema only: still denied, on system.grants"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON information_schema.* TO $user"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT * FROM information_schema.user_privileges" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo "-- with access to system.grants: allowed"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.grants TO $user"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW USERS, SHOW ROLES ON *.* TO $user"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT count() >= 0 FROM information_schema.user_privileges"

${CLICKHOUSE_CLIENT} -q "DROP USER $user"
