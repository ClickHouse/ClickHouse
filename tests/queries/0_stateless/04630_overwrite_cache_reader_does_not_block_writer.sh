#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.overwrite_cache_reader_does_not_block_writer"
reader_pid=""
reader_output=$(mktemp "$CLICKHOUSE_TMP/overwrite-cache-nonblocking-XXXXXX")

cleanup()
{
    if [[ -n "$reader_pid" ]]; then
        wait "$reader_pid" >/dev/null 2>&1 ||:
    fi
    rm -f "$reader_output"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
CREATE TABLE $table (key UInt64, version UInt64, payload UInt64)
ENGINE = OverwriteCache(version)
KEYS (key)
SETTINGS max_memory_bytes = 1073741824"

$CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, 1, 111 FROM numbers(200)"

# A reader that keeps its snapshot open across several publications. Publishing supersedes the rows it
# reads instead of waiting for it, so it must still observe the generation it captured.
$CLICKHOUSE_CLIENT -q "
SELECT sum(payload + sleepEachRow(0.01))
FROM $table
WHERE key IN (SELECT number FROM numbers(200))
SETTINGS max_block_size = 4" > "$reader_output" 2>&1 &
reader_pid=$!

$CLICKHOUSE_CLIENT -q "SELECT sleep(1) FORMAT Null"

for version in 2 3 4 5
do
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, $version, 999 FROM numbers(200)"
done

wait "$reader_pid"
reader_pid=""

if [[ "$(cat "$reader_output")" == "22200" ]]
then
    echo "reader kept its snapshot"
else
    echo "reader saw $(cat "$reader_output"), expected 22200"
fi

$CLICKHOUSE_CLIENT -q "SELECT sum(payload) = 199800 FROM $table WHERE key IN (SELECT number FROM numbers(200))"

# Reading a table while inserting into it must not deadlock: the writer would otherwise wait for a
# reader that cannot finish until the writer does.
$CLICKHOUSE_CLIENT -q "
INSERT INTO $table
SELECT key, 6, 777 FROM $table WHERE key IN (SELECT number FROM numbers(200))"
$CLICKHOUSE_CLIENT -q "SELECT sum(payload) = 155400 FROM $table WHERE key IN (SELECT number FROM numbers(200))"
