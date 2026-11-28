#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: the other replicas cannot create the table, whose constraint reads a named collection only this server has.

# --dump-schema reports a named collection that only a constraint subquery reads, which CREATE runs, also when the
# dumping user cannot see the collection.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

DB="${CLICKHOUSE_DATABASE}"
NC="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DUMP_USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
NC_DDL="CREATE NAMED COLLECTION ${NC} AS url = 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT%201', format = 'TSV', structure = 'x UInt64'"

# The source server runs the subqueries at CREATE, so the collection must resolve there.
$CLICKHOUSE_CLIENT -n -q "
    DROP USER IF EXISTS ${DUMP_USER};
    DROP NAMED COLLECTION IF EXISTS ${NC};
    ${NC_DDL};
    CREATE TABLE ${DB}.t_scalar (id UInt64, CONSTRAINT c CHECK id <= (SELECT count() FROM url(${NC}))) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${DB}.t_in (id UInt64, CONSTRAINT c CHECK id IN (SELECT x FROM url(${NC}))) ENGINE = MergeTree ORDER BY id;
    CREATE USER ${DUMP_USER};
    GRANT SELECT, SHOW TABLES, SHOW COLUMNS ON *.* TO ${DUMP_USER};
    GRANT SHOW DATABASES ON *.* TO ${DUMP_USER};
"

if $CLICKHOUSE_CLIENT --dump-schema "$DB" > "$work/dump.sql" 2>"$work/err"; then
    echo "scalar subquery warned: $(grep -c "t_scalar depends on named collection ${NC}," "$work/err")"
    echo "IN subquery warned: $(grep -c "t_in depends on named collection ${NC}," "$work/err")"
    echo "tables dumped: $(grep -c '^CREATE TABLE' "$work/dump.sql")"
    echo "collections emitted: $(grep -c 'CREATE NAMED COLLECTION' "$work/dump.sql")"
else
    echo "FAIL: dump rejected: $(cat "$work/err")"
fi

# Replay needs the collection the warning names, and nothing else.
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/dump.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
rm -rf "$work/dst"
$CLICKHOUSE_LOCAL --path "$work/dst" -q "$NC_DDL"
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/dump.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
echo "replay with the collection: rc=${PIPESTATUS[0]}"

# A user who cannot see the collection is told the tables may depend on it.
if $CLICKHOUSE_CLIENT --user "$DUMP_USER" --dump-schema "$DB" > /dev/null 2>"$work/err"; then
    echo "hidden, scalar subquery warned: $(grep -c "t_scalar may depend on named collection ${NC}," "$work/err")"
    echo "hidden, IN subquery warned: $(grep -c "t_in may depend on named collection ${NC}," "$work/err")"
else
    echo "FAIL: restricted dump rejected: $(cat "$work/err")"
fi

$CLICKHOUSE_CLIENT -n -q "
    DROP USER ${DUMP_USER};
    DROP TABLE ${DB}.t_scalar;
    DROP TABLE ${DB}.t_in;
    DROP NAMED COLLECTION ${NC};
"
