#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires mysql and postgresql clients

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The MySQL and PostgreSQL handlers answer some user statements without passing them to
# `executeQuery`, or by executing SQL of their own with `query_rules` cleared. Such statements
# must still fail when `query_rules` lists a rule that does not exist, like ordinary SQL does.

RULE="no_such_rule_05325_${CLICKHOUSE_DATABASE}"

# MySQL: query replacements, setting replacements and the federated-setup `SET` no-ops.
for statement in "SHOW WARNINGS" "SHOW VARIABLES" "SET SQL_SELECT_LIMIT = 10" "SET NAMES utf8" "SELECT 1"
do
    echo "mysql: ${statement}"
    ${MYSQL_CLIENT} --execute "SET query_rules = '${RULE}'; ${statement};" 2>&1 | grep -c "${RULE}"
done

# PostgreSQL: the driver no-ops, `PREPARE` and `DEALLOCATE`.
PG_USER="postgresql_user_05325_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

for statement in "BEGIN" "COMMIT" "SET application_name = 'test'" "RESET ALL" "PREPARE p_05325 AS SELECT 1" "DEALLOCATE p_05325" "SELECT 1"
do
    echo "postgresql: ${statement}"
    # Each `-c` is sent as it is, without a terminating `;`, as the drivers send these statements.
    psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only \
        -c "SET query_rules = '${RULE}'" -c "${statement}" 2>&1 \
        | grep -c "${RULE}"
done

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
