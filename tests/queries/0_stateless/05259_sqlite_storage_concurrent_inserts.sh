#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every `INSERT` into `ENGINE = SQLite` writes through its own connection and commits each chunk in an explicit
# transaction, so concurrent inserts into one database compete for its single writer lock. They must wait for
# each other instead of failing with `database is locked` (SQLITE_BUSY).

BASE="${USER_FILES_PATH}/05259_sqlite_concurrent_${CLICKHOUSE_DATABASE}"
DB_PATH="${BASE}/data.sqlite"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05259"
    rm -rf "${BASE}"
}
trap cleanup EXIT

rm -rf "${BASE}"
mkdir -p "${BASE}"

sqlite3 "${DB_PATH}" "CREATE TABLE tbl (writer INTEGER NOT NULL, n INTEGER NOT NULL, s TEXT NOT NULL);"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05259 (writer Int64, n Int64, s String) ENGINE = SQLite('${DB_PATH}', 'tbl')"

# Small blocks turn every insert into many chunk transactions, so the writers interleave.
function insert_rows()
{
    local writer=$1
    for _ in {1..3}; do
        ${CLICKHOUSE_CLIENT} --max_block_size 100 --min_insert_block_size_rows 100 --min_insert_block_size_bytes 0 --query "
            INSERT INTO t_05259 SELECT ${writer}, number, repeat('x', 100) FROM numbers(1000)"
    done
}

for writer in {1..4}; do
    insert_rows "${writer}" &
done
wait

${CLICKHOUSE_CLIENT} --query "SELECT writer, count(), uniqExact(n) FROM t_05259 GROUP BY writer ORDER BY writer"
