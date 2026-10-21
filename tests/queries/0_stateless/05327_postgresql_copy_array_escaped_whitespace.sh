#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# In an unquoted element of a PostgreSQL array literal read by `COPY ... FROM STDIN`, trailing
# whitespace is insignificant, but whitespace escaped with a backslash is part of the element, and an
# escaped `NULL` is the four-character string rather than a null element. The backslash of the array
# literal is doubled in the data, because `COPY` text unescapes it first.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run, so that concurrent runs do not collide.
PG_USER="postgresql_user_05327_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
CREATE TABLE copy_escaped (n UInt8, a Array(Nullable(String))) ENGINE = Memory;
GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.copy_escaped TO ${PG_USER};
"

psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-psqlrc --tuples-only --no-align 2>&1 <<'SQL'
COPY copy_escaped FROM STDIN;
1	{a\\ ,b  ,c\\ \\ , d }
2	{\\NULL,NULL,nUlL ,\\ }
\.
SQL

# A literal with explicit bounds, which PostgreSQL emits for an array whose lower bound is not 1, is
# rejected: a ClickHouse array always starts at index 1, and dropping the bounds would shift the indices.
psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-psqlrc --tuples-only --no-align 2>&1 <<'SQL' | grep -o 'explicit array bounds are not supported'
COPY copy_escaped FROM STDIN;
3	[0:1]={a,b}
\.
SQL

${CLICKHOUSE_CLIENT} -q "
SELECT n, arrayMap(x -> concat('<', x, '>'), a), arrayMap(x -> isNull(x), a) FROM copy_escaped ORDER BY n;
DROP TABLE copy_escaped;
DROP USER ${PG_USER};
"
