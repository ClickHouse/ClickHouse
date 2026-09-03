#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# `$` is a valid character inside an unquoted identifier in both PostgreSQL and ClickHouse, so a
# prepared statement referencing `foo$1bar` references that identifier - the `$1` inside it is not a
# placeholder and must not be rewritten by `EXECUTE`. A statement whose only `$1` text sits inside an
# identifier therefore uses zero parameters, and supplying one is a `wrong number of parameters`
# error, exactly as if the statement contained no placeholder at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04869_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-psqlrc --tuples-only --no-align 2>&1
}

# The identifier `foo$1bar` stays intact while the standalone `$1` next to it is substituted: with
# the argument 42 the result is 5 + 42, not the corrupted identifier `foo42bar`. The word scan does
# not swallow a placeholder after a numeric constant (`1 + $1`), a parenthesis (`($1)`) or an
# operator, and a dollar-quoted string after an identifier keeps working.
run_psql <<'SQL'
PREPARE ident AS SELECT foo$1bar + $1 FROM (SELECT 5 AS foo$1bar);
EXECUTE ident(42);
DEALLOCATE ident;
PREPARE adjacent AS SELECT 1 + $1, ($1), length($$text with $1 inside$$) - $1;
EXECUTE adjacent(10);
DEALLOCATE adjacent;
SQL

# An error terminates the connection on this protocol path, so the wrong-arity case runs in a psql
# invocation of its own, and only the informative part of the error is pinned down.
run_psql <<'SQL' | grep -oE "Wrong number of parameters for prepared statement '[a-z_]+': the statement [a-z]+ [^,]+, but [0-9]+ [a-z()]* ?were supplied"
PREPARE only_inside_identifier AS SELECT foo$1bar FROM (SELECT 5 AS foo$1bar);
EXECUTE only_inside_identifier(42);
SQL

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
