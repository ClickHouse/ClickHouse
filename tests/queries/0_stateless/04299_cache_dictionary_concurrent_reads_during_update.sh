#!/usr/bin/env bash
# Tags: long
#
# Stress test for concurrent reads while the cache dictionary is being updated, evicted and reloaded.
#
# The cache holds far fewer cells than there are distinct keys, so reads constantly miss,
# triggering updates that evict and overwrite cells under the fine-grained locking (the eviction
# scan runs without the reader lock; only the brief per-key commit is exclusive). In parallel a
# loop issues SYSTEM RELOAD DICTIONARY, so reads also race against the dictionary being rebuilt.
#
# Correctness invariant: regardless of cache races, dictGet must ALWAYS return the current source
# value for a key (the cache is only an optimization and re-fetches on miss). The source is
# immutable during the test, so any wrong value returned by a concurrent reader -- a torn cell
# read, a stale element_index, or a map/storage inconsistency -- shows up as a non-zero count of
# mismatches. Both a simple-key and a complex-key cache are exercised. The test passes iff every
# reader sees only correct values. Run under ThreadSanitizer in CI for actual data-race detection.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
DROP DICTIONARY IF EXISTS cache_dict;
DROP DICTIONARY IF EXISTS complex_cache_dict;
DROP TABLE IF EXISTS source;

CREATE TABLE source (id UInt64, tag String, value String) ENGINE = Memory;
INSERT INTO source SELECT number, 'tag_' || toString(number), 'v_' || toString(number) FROM numbers(10000);

-- Small caches (256 cells) vs 10000 distinct keys => constant eviction churn.
CREATE DICTIONARY cache_dict
(
    id UInt64,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source' DB currentDatabase()))
LIFETIME(MIN 0 MAX 1)
LAYOUT(CACHE(SIZE_IN_CELLS 256));

CREATE DICTIONARY complex_cache_dict
(
    id UInt64,
    tag String,
    value String DEFAULT ''
)
PRIMARY KEY id, tag
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source' DB currentDatabase()))
LIFETIME(MIN 0 MAX 1)
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 256));
"

# Readers scan all 10000 keys and return how many dictGet results disagree with the source.
simple_reader() {
    $CLICKHOUSE_CLIENT -q "
        SELECT count()
        FROM numbers(10000)
        WHERE dictGet('cache_dict', 'value', number) != 'v_' || toString(number)
    "
}

complex_reader() {
    $CLICKHOUSE_CLIENT -q "
        SELECT count()
        FROM numbers(10000)
        WHERE dictGet('complex_cache_dict', 'value', (number, 'tag_' || toString(number))) != 'v_' || toString(number)
    "
}

# Background reloader: rebuild both dictionaries (fresh storage) while readers hammer them.
# It runs until killed below; the sleep is only a throttle to avoid a client-process storm.
reloader() {
    while true; do
        $CLICKHOUSE_CLIENT -q "SYSTEM RELOAD DICTIONARY cache_dict" 2>/dev/null
        $CLICKHOUSE_CLIENT -q "SYSTEM RELOAD DICTIONARY complex_cache_dict" 2>/dev/null
        sleep 0.1
    done
}
reloader &
reloader_pid=$!
# Make sure the background reloader can never be orphaned if the script exits early.
trap 'kill "$reloader_pid" 2>/dev/null' EXIT

simple_total=0
complex_total=0
for _ in $(seq 1 4); do
    out=$(for _ in $(seq 1 6); do simple_reader & done; wait)
    for n in $out; do simple_total=$((simple_total + n)); done

    out=$(for _ in $(seq 1 6); do complex_reader & done; wait)
    for n in $out; do complex_total=$((complex_total + n)); done
done

reloader_running=0
kill "$reloader_pid" 2>/dev/null
wait "$reloader_pid" 2>/dev/null

echo "simple mismatches: $simple_total"
echo "complex mismatches: $complex_total"

$CLICKHOUSE_CLIENT -n -q "
DROP DICTIONARY cache_dict;
DROP DICTIONARY complex_cache_dict;
DROP TABLE source;
"
