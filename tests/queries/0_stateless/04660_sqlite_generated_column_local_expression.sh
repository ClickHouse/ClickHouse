#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQLite refuses every write to a generated column, so a local `DEFAULT`/`MATERIALIZED` expression over a
# remote generated column cannot work: ClickHouse would compute the value and send it in the `INSERT`. The
# storage rejects such a declaration instead of letting the insert fail inside SQLite.

DB_PATH="${CLICKHOUSE_TMP}/04660_sqlite.db"
STATE_PATH="${CLICKHOUSE_TMP}/04660_local_state"
rm -rf "${DB_PATH}" "${STATE_PATH}"

sqlite3 "${DB_PATH}" "CREATE TABLE t (a INTEGER, g INTEGER GENERATED ALWAYS AS (a + 1));"

${CLICKHOUSE_LOCAL} --path "${STATE_PATH}" --query "
    CREATE DATABASE IF NOT EXISTS db_04660 ENGINE = Atomic;
    CREATE TABLE db_04660.with_default (a Int64, g Int64 DEFAULT a + 1) ENGINE = SQLite('${DB_PATH}', 't');
" 2>&1 | grep -c -m1 'is a generated column.*INCORRECT_QUERY'

${CLICKHOUSE_LOCAL} --path "${STATE_PATH}" --query "
    CREATE DATABASE IF NOT EXISTS db_04660 ENGINE = Atomic;
    CREATE TABLE db_04660.with_materialized (a Int64, g Int64 MATERIALIZED a + 1) ENGINE = SQLite('${DB_PATH}', 't');
" 2>&1 | grep -c -m1 'is a generated column.*INCORRECT_QUERY'

# An ordinary declaration of the same column keeps working: the storage classifies it as `MATERIALIZED` on its
# own and SQLite computes the value.
${CLICKHOUSE_LOCAL} --path "${STATE_PATH}" --query "
    CREATE DATABASE IF NOT EXISTS db_04660 ENGINE = Atomic;
    CREATE TABLE db_04660.ordinary (a Int64, g Int64) ENGINE = SQLite('${DB_PATH}', 't');

    INSERT INTO db_04660.ordinary (a) VALUES (1);

    SELECT 'An ordinary declaration is reclassified and readable:';
    SELECT a, g FROM db_04660.ordinary ORDER BY a;
"

rm -rf "${DB_PATH}" "${STATE_PATH}"
