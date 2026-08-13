#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: rewrite rules are global server state
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The SQL that implements the PostgreSQL wire protocol's `COPY ... FROM STDIN` /
# `COPY ... TO STDOUT` is synthesized by the server; the user never submitted it, so an active
# `query_rules` matching that synthesized text must not reject (or rewrite) the COPY.

PG_USER="postgresql_user_04872_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT, INSERT, CREATE, DROP ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.tbl_04872;
CREATE TABLE ${CLICKHOUSE_DATABASE}.tbl_04872 (val UInt32) ENGINE=MergeTree ORDER BY val;

CREATE RULE rule_04872_copy_from AS (INSERT INTO tbl_04872 FROM INFILE 'psql_copy') REJECT WITH 'blocked_04872_from';
CREATE RULE rule_04872_copy_to AS (SELECT * FROM tbl_04872) REJECT WITH 'blocked_04872_to';
"

# Sanity check: the rules do fire on the same SQL when the user submits it directly (the rule
# template is unqualified, like the SQL the server synthesizes for COPY, so match it unqualified).
${CLICKHOUSE_CLIENT} -q "SET query_rules = 'rule_04872_copy_to'; SELECT * FROM tbl_04872;" 2>&1 \
    | grep -o -m1 'blocked_04872_to'

# The COPY statements must succeed even though the session activates the rules matching their
# synthesized implementation SQL.
psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only 2>&1 <<'EOF'
SET query_rules = 'rule_04872_copy_from, rule_04872_copy_to';
COPY tbl_04872 FROM STDIN;
1
2
3
\.
COPY tbl_04872 TO STDOUT;
SELECT count() AS copied_rows FROM tbl_04872;
EOF

${CLICKHOUSE_CLIENT} -q "
DROP RULE rule_04872_copy_from;
DROP RULE rule_04872_copy_to;
DROP TABLE ${CLICKHOUSE_DATABASE}.tbl_04872;
DROP USER ${PG_USER};
"
