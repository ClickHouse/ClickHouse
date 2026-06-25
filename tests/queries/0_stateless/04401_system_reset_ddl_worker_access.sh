#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SYSTEM RESET DDL WORKER must require the SYSTEM RESET DDL WORKER privilege.
# Unique, database-scoped user names so the test is safe to run in parallel with itself.
user_unpriv="u_unpriv_${CLICKHOUSE_DATABASE}"
user_priv="u_priv_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_unpriv}, ${user_priv}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_unpriv}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_priv}"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM RESET DDL WORKER ON *.* TO ${user_priv}"

# Unprivileged user is denied.
${CLICKHOUSE_CLIENT} --user="${user_unpriv}" --query "SYSTEM RESET DDL WORKER" 2>&1 \
    | grep -q "ACCESS_DENIED" && echo "unprivileged: denied" || echo "unprivileged: NOT denied"

# Granted user passes the access check. On a server without a distributed DDL
# configuration the command then fails with NO_ELEMENTS_IN_CONFIG (not ACCESS_DENIED),
# which confirms the privilege check succeeded rather than blocking the user.
${CLICKHOUSE_CLIENT} --user="${user_priv}" --query "SYSTEM RESET DDL WORKER" 2>&1 \
    | grep -q "ACCESS_DENIED" && echo "granted: denied" || echo "granted: allowed"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user_unpriv}, ${user_priv}"
