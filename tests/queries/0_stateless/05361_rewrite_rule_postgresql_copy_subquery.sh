#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: rewrite rules are global server state
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query of `COPY (query) TO STDOUT` is SQL the user submitted, so the session's `query_rules`
# apply to it. Only the `SELECT` the server synthesizes for `COPY table TO STDOUT` is exempt.

PG_USER="postgresql_user_05361_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.tbl_05361;
CREATE TABLE ${CLICKHOUSE_DATABASE}.tbl_05361 (val UInt32) ENGINE=MergeTree ORDER BY val;
INSERT INTO ${CLICKHOUSE_DATABASE}.tbl_05361 VALUES (1);
CREATE RULE rule_05361 AS (SELECT val FROM tbl_05361) REJECT WITH 'blocked_05361';
" < /dev/null

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only 2>&1 <<'EOF' | grep -o -E 'blocked_05361|^[A-Z0-9]+$'
SET query_rules = 'rule_05361';
COPY (SELECT val FROM tbl_05361) TO STDOUT;
COPY tbl_05361 TO STDOUT;
EOF

${CLICKHOUSE_CLIENT} -q "
DROP RULE rule_05361;
DROP TABLE ${CLICKHOUSE_DATABASE}.tbl_05361;
DROP USER ${PG_USER};
" < /dev/null
