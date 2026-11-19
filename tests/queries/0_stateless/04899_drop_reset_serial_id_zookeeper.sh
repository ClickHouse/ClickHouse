#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

series="${CLICKHOUSE_DATABASE}_serial_drop_reset"

${CLICKHOUSE_CLIENT} --query "SELECT generateSerialID('${series}')"
${CLICKHOUSE_CLIENT} --query "SELECT generateSerialID('${series}')"
${CLICKHOUSE_CLIENT} --query "SYSTEM RESET SERIAL ID '${series}' TO 10"
${CLICKHOUSE_CLIENT} --query "SELECT generateSerialID('${series}')"
${CLICKHOUSE_CLIENT} --query "SYSTEM RESET SERIAL ID '${series}'"
${CLICKHOUSE_CLIENT} --query "SELECT generateSerialID('${series}')"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP SERIAL ID '${series}'"
${CLICKHOUSE_CLIENT} --query "SELECT generateSerialID('${series}')"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP SERIAL ID IF EXISTS '${series}_missing'"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP SERIAL ID '${series}_missing'" 2>&1 \
    | grep -c "Serial ID series"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP SERIAL ID '${series}'"

# RESET creates the series when it does not exist.
${CLICKHOUSE_CLIENT} --query "SYSTEM RESET SERIAL ID '${series}_created' TO 7"
${CLICKHOUSE_CLIENT} --query "SELECT generateSerialID('${series}_created')"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP SERIAL ID '${series}_created'"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP SERIAL ID ''" 2>&1 \
    | grep -c "cannot be empty"
${CLICKHOUSE_CLIENT} --query "SYSTEM RESET SERIAL ID ''" 2>&1 \
    | grep -c "cannot be empty"

user_unpriv="u_serial_unpriv_${CLICKHOUSE_DATABASE}"
user_priv="u_serial_priv_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_unpriv}, ${user_priv}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_unpriv}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_priv}"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM DROP SERIAL ID, SYSTEM RESET SERIAL ID ON *.* TO ${user_priv}"

${CLICKHOUSE_CLIENT} --user="${user_unpriv}" --query "SYSTEM DROP SERIAL ID '${series}_acl'" 2>&1 \
    | grep -q "ACCESS_DENIED" && echo "unprivileged drop: denied" || echo "unprivileged drop: NOT denied"
${CLICKHOUSE_CLIENT} --user="${user_unpriv}" --query "SYSTEM RESET SERIAL ID '${series}_acl'" 2>&1 \
    | grep -q "ACCESS_DENIED" && echo "unprivileged reset: denied" || echo "unprivileged reset: NOT denied"
${CLICKHOUSE_CLIENT} --user="${user_priv}" --query "CHECK GRANT SYSTEM DROP SERIAL ID ON *.*" \
    | grep -q "^1$" && echo "granted drop: has privilege" || echo "granted drop: missing privilege"
${CLICKHOUSE_CLIENT} --user="${user_priv}" --query "CHECK GRANT SYSTEM RESET SERIAL ID ON *.*" \
    | grep -q "^1$" && echo "granted reset: has privilege" || echo "granted reset: missing privilege"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user_unpriv}, ${user_priv}"
