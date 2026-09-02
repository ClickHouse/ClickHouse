#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# `ParserExecute` dereferenced the result of a cast to `ASTLiteral` without checking it, so an
# `EXECUTE` statement with an argument that is not a literal dereferenced a null pointer.

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

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align 2>&1 <<'EOF' | grep -c -F 'Syntax error'
PREPARE p AS SELECT 1;
EXECUTE p(1 + 1);
EOF

# The server must have survived the malformed statement.
${CLICKHOUSE_CLIENT} -q "SELECT 'alive'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
