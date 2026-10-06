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

# Each `COPY ... FROM STDIN` gets an invocation of its own, so that end of input terminates the
# data: a `psql` that forwards the `\.` marker to the server instead of consuming it would hand
# the server a row that is not data.
psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only --quiet 2>&1 <<'EOF2'
COPY tbl_05331 FROM STDIN;
2	b
EOF2

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only --quiet 2>&1 <<'EOF2'
COPY tbl_05331 (id, s) FROM STDIN WITH (FORMAT csv);
3,c
EOF2

${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ${CLICKHOUSE_DATABASE}.tbl_05331 FINAL"

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align --tuples-only --quiet 2>&1 <<'EOF2'
COPY tbl_05331 TO STDOUT;
COPY tbl_05331 TO STDOUT WITH (FORMAT csv);
EOF2

${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${CLICKHOUSE_DATABASE}.tbl_05331;
DROP USER ${PG_USER};
"
