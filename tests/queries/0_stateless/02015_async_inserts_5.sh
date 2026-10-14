#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts (id UInt32, s String) ENGINE = MergeTree ORDER BY id SETTINGS parts_to_throw_insert = 1"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES $CLICKHOUSE_DATABASE.async_inserts"

# This insert seeds the single part that makes `parts_to_throw_insert = 1` reject the async inserts
# below, so it must succeed itself. `max_insert_threads > 1` (which the test runner randomizes) builds
# one `MergeTreeSink` per thread, and every sink re-checks the part limit in its own `onStart`: the
# first sink writes and commits the part, and a sink scheduled after that commit already sees one
# active part and throws. Pin a single insert thread so this query cannot outrace itself.
${CLICKHOUSE_CLIENT} -q "INSERT INTO async_inserts SETTINGS max_insert_threads = 1 VALUES (1, 's')"

for _ in {1..3}; do
    ${CLICKHOUSE_CURL} -sS $url -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 2, "s": "a"} {"id": 3, "s": "b"}' \
        | grep -o "Too many parts" &
done

wait

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_inserts"
