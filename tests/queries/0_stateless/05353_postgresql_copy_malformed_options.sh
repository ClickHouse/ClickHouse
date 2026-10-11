#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `COPY` with malformed option syntax or an alias of its query is rejected rather than run with the
# options that were understood, a qualified call of a function named `pg_table_is_visible` is not
# rewritten into the catalog check, and `format_type` takes a `NULL` type modifier.

PG_USER="postgresql_user_05353_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
CREATE TABLE ${CLICKHOUSE_DATABASE}.tbl_05353 (id UInt32, s String) ENGINE = Memory;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.tbl_05353 TO ${PG_USER};
INSERT INTO ${CLICKHOUSE_DATABASE}.tbl_05353 VALUES (1, 'a');
"

PSQL=(psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only --quiet)

# Well-formed options still work.
"${PSQL[@]}" -c "COPY tbl_05353 TO STDOUT WITH (FORMAT csv, HEADER);" 2>&1
"${PSQL[@]}" -c "COPY tbl_05353 TO STDOUT WITH CSV HEADER DELIMITER AS ',';" 2>&1
"${PSQL[@]}" -c "COPY (SELECT 2) TO STDOUT;" 2>&1

for query in \
    "COPY tbl_05353 TO STDOUT WITH (FORMAT csv, false)" \
    "COPY tbl_05353 TO STDOUT WITH CSV true" \
    "COPY tbl_05353 TO STDOUT AS" \
    "COPY tbl_05353 TO STDOUT WITH CSV WITH"
do
    "${PSQL[@]}" -c "${query}" 2>&1 | grep -o -m1 "malformed option syntax"
done

# The parentheses that do not balance in the whole query are rejected by the lexer already.
"${PSQL[@]}" -c "COPY tbl_05353 TO STDOUT WITH (" 2>&1 | grep -o -m1 "Unmatched parentheses"

# PostgreSQL has no alias in `COPY (query)`.
"${PSQL[@]}" -c "COPY (SELECT 1) AS junk TO STDOUT" 2>&1 | grep -o -m1 "Syntax error"
"${PSQL[@]}" -c "COPY (SELECT 1) junk TO STDOUT" 2>&1 | grep -o -m1 "Syntax error"

# Only the catalog function is rewritten; a function of another schema is left as it is.
"${PSQL[@]}" -c "SELECT other.pg_table_is_visible(1)" 2>&1 | grep -o -m1 "other.pg_table_is_visible\` does not exist"
"${PSQL[@]}" -c "SELECT pg_catalog.pg_table_is_visible(1)" 2>&1

# A `NULL` type modifier means that no modifier is known.
"${PSQL[@]}" -c "SELECT format_type(1700, NULL), format_type(1700, 655366), format_type(NULL, 1) IS NULL" 2>&1
${CLICKHOUSE_CLIENT} -q "
SELECT format_type(1700, NULL), toTypeName(format_type(1700, NULL)), format_type(NULL, -1);
SELECT format_type(materialize(toNullable(1700)), materialize(CAST(NULL, 'Nullable(Int32)'))), format_type(materialize(23), materialize(toNullable(-1)));
"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${CLICKHOUSE_DATABASE}.tbl_05353;
DROP USER ${PG_USER};
"
