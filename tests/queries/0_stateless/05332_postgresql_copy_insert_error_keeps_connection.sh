#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# A `COPY ... FROM STDIN` whose payload parses but whose insert fails after `CopyDone` (here a
# violated `CONSTRAINT`) is an ordinary error of the statement: the server must answer with an
# `ErrorResponse` and keep the connection usable, so the next statement runs in the same session.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run, so that concurrent runs do not collide.
PG_USER="postgresql_user_05332_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
CREATE TABLE copy_insert_error (n UInt8, CONSTRAINT small CHECK n < 10) ENGINE = Memory;
GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.copy_insert_error TO ${PG_USER};
"

# A session setting survives only if the connection is not reopened.
psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-psqlrc --tuples-only --no-align 2>&1 <<'SQL' | sed -E 's/^(ERROR:  COPY FROM STDIN failed:).*(VIOLATED_CONSTRAINT).*/\1 \2/'
SET max_threads = 3;
COPY copy_insert_error FROM STDIN;
1
20
\.
SELECT 'connection is usable', getSetting('max_threads');
COPY copy_insert_error FROM STDIN;
2
3
\.
SELECT count() FROM copy_insert_error;
SQL

${CLICKHOUSE_CLIENT} -q "
SELECT n FROM copy_insert_error ORDER BY n;
DROP TABLE copy_insert_error;
DROP USER ${PG_USER};
"
