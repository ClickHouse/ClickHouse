#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# PostgreSQL-style placeholder substitution in `EXECUTE`: every occurrence of a placeholder is
# replaced (a parameter may be referenced several times), the placeholder number is read as a whole
# (`$10` is the tenth parameter, not `$1` followed by `0`), a `$1` inside a string literal or a
# comment is ordinary text, and the argument count must match the statement exactly - both a missing
# and an extra argument are `wrong number of parameters` errors, instead of a raw `$n` reaching
# execution.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04836_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-psqlrc --tuples-only --no-align 2>&1
}

# All the statements of one psql invocation run on one connection, which is what scopes a prepared
# statement.
run_psql <<'SQL'
PREPARE repeated AS SELECT $1 + $1 + $1;
EXECUTE repeated(14);
DEALLOCATE repeated;
PREPARE many AS SELECT $10 - $2 - $1;
EXECUTE many(1, 2, 0, 0, 0, 0, 0, 0, 0, 45);
DEALLOCATE many;
PREPARE quoted AS SELECT '$1 stays literal', $1;
EXECUTE quoted(42);
DEALLOCATE quoted;
PREPARE commented AS SELECT /* $2 is no placeholder here */ $1 -- neither is $3
;
EXECUTE commented(42);
DEALLOCATE commented;
PREPARE no_params AS SELECT 42;
EXECUTE no_params;
DEALLOCATE no_params;
SQL

# An error terminates the connection on this protocol path, so each wrong-arity case runs in a psql
# invocation of its own, and only the informative part of the error is pinned down.
run_psql <<'SQL' | grep -oE "Wrong number of parameters for prepared statement '[a-z_]+': the statement [a-z]+ [^,]+, but [0-9]+ [a-z()]* ?were supplied"
PREPARE too_few AS SELECT $1 + $2;
EXECUTE too_few(1);
SQL

run_psql <<'SQL' | grep -oE "Wrong number of parameters for prepared statement '[a-z_]+': the statement [a-z]+ [^,]+, but [0-9]+ [a-z()]* ?were supplied"
PREPARE too_many AS SELECT $1;
EXECUTE too_many(1, 2);
SQL

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
