#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: rewrite rules are global server state
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `COPY` statement is SQL the client submitted, so the `query_rules` gate applies to it like
# to any other user query — only the `INSERT` / `SELECT` the server synthesizes to implement the
# wire protocol is exempt (see `04872_rewrite_rule_postgresql_copy_synthesized_sql`). A rule name
# listed in `query_rules` that does not exist must therefore fail a `COPY` too, instead of being
# silently skipped.

PG_USER="postgresql_user_05055_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT, INSERT, CREATE, DROP ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.tbl_05055;
CREATE TABLE ${CLICKHOUSE_DATABASE}.tbl_05055 (val UInt32) ENGINE=MergeTree ORDER BY val;
INSERT INTO ${CLICKHOUSE_DATABASE}.tbl_05055 VALUES (1);
"

# Reference behaviour of an ordinary statement with a missing rule name.
psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only 2>&1 <<'EOF2' | grep -o -m1 'no_such_rule_05055'
SET query_rules = 'no_such_rule_05055';
SELECT 1;
EOF2

# `COPY ... TO STDOUT` and `COPY ... FROM STDIN` must be gated the same way.
psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only 2>&1 <<'EOF2' | grep -o -m1 'no_such_rule_05055'
SET query_rules = 'no_such_rule_05055';
COPY tbl_05055 TO STDOUT;
EOF2

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only 2>&1 <<'EOF2' | grep -o -m1 'no_such_rule_05055'
SET query_rules = 'no_such_rule_05055';
COPY tbl_05055 FROM STDIN;
2
\.
EOF2

# The rows are unchanged: the rejected `COPY FROM` inserted nothing.
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM ${CLICKHOUSE_DATABASE}.tbl_05055;
DROP TABLE ${CLICKHOUSE_DATABASE}.tbl_05055;
DROP USER ${PG_USER};
"
