#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# `ParserExecute` dereferenced the result of a cast to `ASTLiteral` without checking it, so an
# `EXECUTE` statement with an argument that is not a literal dereferenced a null pointer. A non-literal
# argument is accepted now (PostgreSQL allows an arbitrary expression there, see
# `04824_postgresql_execute_expression_arguments`), so what the statement below is rejected for is the
# arity mismatch - the point of the test is that the server reports the error and survives.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times
# concurrently, and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_05036_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align 2>&1 <<'EOF' | grep -c -F 'the statement uses 0 parameter(s), but 1 were supplied'
PREPARE p AS SELECT 1;
EXECUTE p(1 + 1);
EOF

# The server must have survived the malformed statement.
${CLICKHOUSE_CLIENT} -q "SELECT 'alive'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
