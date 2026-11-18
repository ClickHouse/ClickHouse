#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-ordinary-database, no-fasttest
# no-parallel: the PAUSEABLE_ONCE failpoint fires once globally.
# Regression test for #132379: a cross-database RENAME/EXCHANGE must not move a table out of a
# database while DROP DATABASE is preparing and dropping its tables. Otherwise the drop shuts down
# a table that survives in another database, and that table stays read-only / never merges.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB_A="${CLICKHOUSE_DATABASE}_drop_a"
DB_B="${CLICKHOUSE_DATABASE}_drop_b"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT drop_database_holding_drop_lock" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_A}" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_B}" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB_A};
    CREATE DATABASE ${DB_B};
    CREATE TABLE ${DB_A}.t (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE ${DB_A}.s (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE ${DB_B}.u (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO ${DB_A}.t VALUES (1);
    INSERT INTO ${DB_B}.u VALUES (1);
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT drop_database_holding_drop_lock"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB_A} SYNC" &
DROP_PID=$!

${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT drop_database_holding_drop_lock PAUSE"

# While the drop is running, moving a table out of (or into) the dropped database is refused.
${CLICKHOUSE_CLIENT} -q "RENAME TABLE ${DB_A}.t TO ${DB_B}.t" 2>&1 | grep -o -m1 'UNFINISHED'
${CLICKHOUSE_CLIENT} -q "RENAME TABLE ${DB_B}.u TO ${DB_A}.u" 2>&1 | grep -o -m1 'UNFINISHED'
${CLICKHOUSE_CLIENT} -q "EXCHANGE TABLES ${DB_A}.t AND ${DB_B}.u" 2>&1 | grep -o -m1 'UNFINISHED'

# A rename inside one database is still allowed.
${CLICKHOUSE_CLIENT} -q "RENAME TABLE ${DB_B}.u TO ${DB_B}.u2"
${CLICKHOUSE_CLIENT} -q "RENAME TABLE ${DB_B}.u2 TO ${DB_B}.u"

${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT drop_database_holding_drop_lock"
wait $DROP_PID

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${DB_A}'"
# The table of the other database is untouched, still writable and mergeable.
${CLICKHOUSE_CLIENT} -nm -q "
    INSERT INTO ${DB_B}.u VALUES (2);
    OPTIMIZE TABLE ${DB_B}.u FINAL;
    SELECT count() FROM ${DB_B}.u;
"
# Once the drop is over, cross-database renames work again.
${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB_A};
    RENAME TABLE ${DB_B}.u TO ${DB_A}.u;
    SELECT count() FROM ${DB_A}.u;
"
