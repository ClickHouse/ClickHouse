#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `psql` sends a `COPY` with the `;` that terminates it, which must not be taken for an unknown
# part of the command.

PG_USER="postgresql_user_05331_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
CREATE TABLE ${CLICKHOUSE_DATABASE}.tbl_05331 (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.tbl_05331 VALUES (1, 'a');
"

# The data is read from the standard input until its end, so the `\.` that marks the end of the
# data in a script is not needed: `psql` before version 18 sends that marker on to the server.
PSQL=(psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only --quiet)

printf '2\tb\n' | "${PSQL[@]}" -c "COPY tbl_05331 FROM STDIN;" 2>&1
printf '3,c\n' | "${PSQL[@]}" -c "COPY tbl_05331 (id, s) FROM STDIN WITH (FORMAT csv);" 2>&1
printf '4,d\n' | "${PSQL[@]}" -c "COPY tbl_05331 FROM STDIN WITH CSV;" 2>&1

${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ${CLICKHOUSE_DATABASE}.tbl_05331 FINAL"

"${PSQL[@]}" 2>&1 <<'EOF2'
COPY tbl_05331 TO STDOUT;
COPY tbl_05331 TO STDOUT WITH (FORMAT csv);
COPY tbl_05331 TO STDOUT WITH CSV;
EOF2

# The `;` ends the command only after `STDOUT` or `STDIN`, which is not optional.
"${PSQL[@]}" -c "COPY tbl_05331 TO;" 2>&1 | grep -o -m1 "Syntax error"
"${PSQL[@]}" -c "COPY tbl_05331 FROM;" 2>&1 | grep -o -m1 "Syntax error"
"${PSQL[@]}" -c "COPY (SELECT 1) TO;" 2>&1 | grep -o -m1 "Syntax error"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${CLICKHOUSE_DATABASE}.tbl_05331;
DROP USER ${PG_USER};
"
