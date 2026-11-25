#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ALTER of the source column type between two blocks of one INSERT. The view query must be
# resolved against the pushed block, not against the table's current metadata.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS dst;
    DROP TABLE IF EXISTS mv;
    CREATE TABLE src (arr Array(Tuple(a UInt32))) ENGINE = Null;
    CREATE TABLE dst (cnt UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT count() AS cnt FROM src WHERE arrayExists(x -> x.a >= 0, arr);
"

# One row per block, 0.2 s apart, so the INSERT lasts about 6 s.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO src
    SELECT [tuple(toUInt32(number))]
    FROM numbers(30)
    WHERE NOT ignore(sleepEachRow(0.2))
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1, max_threads = 1, max_insert_threads = 1
" &

# Wait until the first block has passed through the view, so the INSERT surely started with the old type.
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM dst") -lt 1 ]]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "ALTER TABLE src MODIFY COLUMN arr Array(Tuple(a UInt32, b Nullable(UInt32)))"
wait

$CLICKHOUSE_CLIENT -q "SELECT sum(cnt) FROM dst"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv;
    DROP TABLE dst;
    DROP TABLE src;
"
