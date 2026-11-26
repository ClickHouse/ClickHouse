#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-block-cache.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# A set on disk caches the blocks that its lookups read, so that later lookups do not read them again. With a
# threshold of 1 byte, each set spills before its first chunk.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --max_bytes_before_external_set 1 <<'SQL'
-- The set holds the even numbers below 200000 as packed gaps, and lookups decode cached blocks as they decode
-- blocks read from the file. Looking up the remainders of the numbers below 20000 divided by 2000, multiplied
-- by 99, must find the 10000 even numbers. The chunks of 1000 lookups alternate between the remainders below
-- 1000 and the others, so each chunk after the second gets the blocks of an earlier chunk from the cache.
SELECT 'packed', countIf(found), sumIf(number, found)
FROM (SELECT number, (number % 2000) * 99 IN (SELECT number * 2 FROM numbers(100000)) AS found FROM numbers(20000))
SETTINGS max_block_size = 1000, log_comment = 'packed';

-- The set stores the hashes of the even numbers below 200000 as they are, in about 200 blocks, and looking up
-- the hashes of the remainders of the numbers below 20000 divided by 2000 must find the 10000 even numbers.
-- Each chunk of 100 lookups needs the blocks of its 50 keys that the set has. Without the cache, each chunk
-- reads all the blocks that it needs. The default cache holds the whole set, so each block is read once and
-- later chunks get it from the cache. A cache of 200000 bytes holds about 50 blocks, so it evicts blocks that
-- later chunks need, and those chunks read them again. Each block that a chunk needs is either read or taken
-- from the cache. The lookups run in one thread, so the chunks are the same in each query.
SELECT 'cache', count(), sum(number) FROM numbers(20000) WHERE cityHash64(number % 2000) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_threads = 1, max_block_size = 100, log_comment = 'whole set';
SELECT 'cache', count(), sum(number) FROM numbers(20000) WHERE cityHash64(number % 2000) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_threads = 1, max_block_size = 100, max_bytes_for_external_set_cache = 200000, log_comment = 'small cache';
SELECT 'cache', count(), sum(number) FROM numbers(20000) WHERE cityHash64(number % 2000) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_threads = 1, max_block_size = 100, max_bytes_for_external_set_cache = 0, log_comment = 'no cache';

-- The lookups of all threads share the cache, which can evict a block that one thread took from it and has
-- not decoded yet.
SELECT 'threads', count(), sum(number)
FROM numbers_mt(200000) WHERE cityHash64(number % 2000) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_threads = 8, max_block_size = 100, max_bytes_for_external_set_cache = 200000, log_comment = 'threads';

-- Every set spilled.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

-- The lookups into the packed set took blocks from the cache, and the lookups without a cache took none. The
-- reads below show the hits of the other queries.
SELECT log_comment, ProfileEvents['ExternalSetCacheHits'] > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('packed', 'no cache')
    AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

-- Lookups read fewer blocks with a larger cache, and the blocks read plus the blocks taken from the cache add
-- up to the blocks read without a cache.
WITH reads AS
(
    SELECT log_comment, ProfileEvents['ExternalSetReadBlocks'] AS blocks, ProfileEvents['ExternalSetCacheHits'] AS hits
    FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase()
)
SELECT
    (SELECT blocks FROM reads WHERE log_comment = 'whole set') < (SELECT blocks FROM reads WHERE log_comment = 'small cache'),
    (SELECT blocks FROM reads WHERE log_comment = 'small cache') < (SELECT blocks FROM reads WHERE log_comment = 'no cache'),
    (SELECT blocks + hits FROM reads WHERE log_comment = 'whole set') = (SELECT blocks FROM reads WHERE log_comment = 'no cache'),
    (SELECT blocks + hits FROM reads WHERE log_comment = 'small cache') = (SELECT blocks FROM reads WHERE log_comment = 'no cache');
SQL
