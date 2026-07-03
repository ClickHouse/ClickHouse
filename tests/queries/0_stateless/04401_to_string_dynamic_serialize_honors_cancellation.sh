#!/usr/bin/env bash
# Serializing a composite value (Dynamic/Array/...) to text via toString runs a per-row loop that used to ignore
# query cancellation (it was only checked between pipeline blocks). A single large block of such values kept a
# thread busy for a long time after KILL QUERY, tripping the "Hung check failed, possible deadlock found" stress
# check. The loop now checks for cancellation per row, so KILL QUERY ... SYNC returns quickly instead of blocking
# until the whole block finishes serializing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="04401_${CLICKHOUSE_DATABASE}"

# One big block (max_block_size covers all rows) of deeply nested Array values wrapped in Dynamic. Building the
# input takes ~2s and serializing it to text takes several more seconds; FORMAT Null discards the output. A per-row
# cancellation check inside the serialization loop is the only thing that can stop it before the whole block is done.
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "
    SELECT toString(arrayMap(z -> arrayMap(y -> range(y % 4), range(z % 7)), range(number % 20))::Dynamic)
    FROM numbers(8000000)
    FORMAT Null
    SETTINGS max_block_size = 8000000, max_threads = 1, max_memory_usage = 0" >/dev/null 2>&1 &
bg_pid=$!

# Wait for the query to start, then let it get past input building and into the serialization loop.
for _ in {1..200}; do
    [[ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" == "1" ]] && break
    sleep 0.1
done
sleep 3

# KILL ... SYNC waits until the query has actually stopped. Time how long that takes: with the per-row check it
# returns in well under a second; without it, it blocks until the serialization loop finishes (several seconds).
start=$EPOCHREALTIME
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' SYNC" >/dev/null 2>&1
end=$EPOCHREALTIME
wait "$bg_pid" 2>/dev/null

awk -v d="$(echo "$end - $start" | bc)" 'BEGIN { if (d < 2) print "cancelled promptly"; else printf "KILL blocked for %.1fs\n", d }'
