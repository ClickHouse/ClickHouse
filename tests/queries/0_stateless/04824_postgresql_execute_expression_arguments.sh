#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# In PostgreSQL, the parameters of a simple-query `EXECUTE` are arbitrary expressions, not only
# literals (`EXECUTE s(1 + 1)`). The parameters are substituted into the prepared statement body as
# their (parenthesized) SQL text, so an expression parameter must execute cleanly instead of tripping
# over an assumption that every parameter is a literal.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04824_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

# All the statements of one psql invocation run on one connection, which is what scopes a prepared
# statement. `--tuples-only --no-align` prints the bare values.
psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-psqlrc --tuples-only --no-align <<'SQL'
PREPARE add_params AS SELECT $1 + $2;
EXECUTE add_params(1 + 1, 40);
DEALLOCATE add_params;
PREPARE one_param AS SELECT $1;
EXECUTE one_param(2 * 3 + 1);
DEALLOCATE one_param;
PREPARE str_param AS SELECT upper($1);
EXECUTE str_param(concat('ab', 'c'));
DEALLOCATE str_param;
PREPARE literal_still_works AS SELECT $1 + 1;
EXECUTE literal_still_works(41);
DEALLOCATE literal_still_works;
SQL

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
