#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILTER_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_table_filter_XXXXXX.sqlite")
DIRECT_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_txn_XXXXXX.sqlite")
trap 'rm -f "$FILTER_DB" "$DIRECT_DB"' EXIT

# --- Default table resolution must treat the `_` in `sqlite_` as a literal, not a wildcard ---
# A user table named `sqliteX` must be picked, while genuine internal `sqlite_*` tables stay
# excluded. The internal `sqlite_sequence` table is forced to exist and to sort before `sqliteX`
# (it would be chosen first if the filter were wrong) by creating an AUTOINCREMENT table and
# dropping it again.
rm -f "$FILTER_DB"
sqlite3 "$FILTER_DB" "CREATE TABLE seqdriver(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);"
sqlite3 "$FILTER_DB" "INSERT INTO seqdriver(v) VALUES ('x');"
sqlite3 "$FILTER_DB" "DROP TABLE seqdriver;"
sqlite3 "$FILTER_DB" "CREATE TABLE sqliteX(id INTEGER, name TEXT);"
sqlite3 "$FILTER_DB" "INSERT INTO sqliteX VALUES (1, 'a'), (2, 'b');"

echo 'Tables present (internal table sorts first):'
sqlite3 "$FILTER_DB" "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY rowid;"

echo 'Default resolution reads the user table sqliteX:'
${CLICKHOUSE_LOCAL} --query "SELECT id, name FROM file('$FILTER_DB', 'SQLite', 'id Int64, name String') ORDER BY id"

# --- A failed direct local-file `FORMAT SQLite` write must not leave a committed table behind ---
# The `CREATE TABLE` and the row inserts must commit or roll back together. The failure is injected
# on a later block (squashing disabled, one row per block) so that the table has already been
# created inside the transaction by the time the write aborts; the whole transaction must roll back.
rm -f "$DIRECT_DB"
echo 'Failed write reports the error:'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('$DIRECT_DB', 'SQLite', 'x UInt64')
    SELECT throwIf(number = 2, 'injected failure') AS x FROM numbers(5)
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0" 2>&1 \
    | grep -oF 'injected failure' | head -1

echo 'No committed table is left behind after the failed write:'
sqlite3 "$DIRECT_DB" "SELECT count(*) FROM sqlite_master WHERE type = 'table';"
