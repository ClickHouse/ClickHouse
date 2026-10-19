#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-filter.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# A set on disk keeps Bloom filters of its keys, which let lookups of keys that the set does not have skip
# reading blocks. With a threshold of 1 byte, each set spills before its first chunk.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --max_bytes_before_external_set 1 <<'SQL'
-- The filters must hold every key of the set, whatever the width of its keys. Each set holds the keys of the
-- even numbers below 200000, and the lookups of the numbers below 20000 must find the 10000 even ones.
SELECT 'UInt64', countIf(found), sumIf(number, found)
FROM (SELECT number, cityHash64(number) IN (SELECT cityHash64(number * 2) FROM numbers(100000)) AS found FROM numbers(20000))
SETTINGS log_comment = 'UInt64';
SELECT 'String', countIf(found), sumIf(number, found)
FROM (SELECT number, toString(number) IN (SELECT toString(number * 2) FROM numbers(100000)) AS found FROM numbers(20000))
SETTINGS log_comment = 'String';
SELECT 'UInt256', countIf(found), sumIf(number, found)
FROM
(
    SELECT number, (number, number, number, number) IN (SELECT number * 2, number * 2, number * 2, number * 2 FROM numbers(100000)) AS found
    FROM numbers(20000)
)
SETTINGS log_comment = 'UInt256';

-- The set of 100000 hashes takes about 200 blocks. Each chunk of 100 lookups of keys that the set does not
-- have needs a block for each key without filters, and only the blocks of the keys that the filters let pass
-- with them. Without the cache, each chunk reads its blocks again, so the reads count the keys that
-- the filters let pass. The filters take 1 byte per key, and each key sets 3 bits. A smaller
-- `max_bytes_for_external_set_bloom_filter` leaves fewer bits per key, and each key sets fewer of them: 2
-- with 3 bits per key, and 1 with 2 bits per key. A budget below 1 bit per key leaves the set without
-- filters, as does a budget of 0.
SELECT 'misses', count() FROM numbers(10000) WHERE cityHash64(number * 2 + 1) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_block_size = 100, max_bytes_for_external_set_cache = 0, log_comment = 'filter';
SELECT 'misses', count() FROM numbers(10000) WHERE cityHash64(number * 2 + 1) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_block_size = 100, max_bytes_for_external_set_cache = 0, max_bytes_for_external_set_bloom_filter = 40000, log_comment = '3 bits';
SELECT 'misses', count() FROM numbers(10000) WHERE cityHash64(number * 2 + 1) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_block_size = 100, max_bytes_for_external_set_cache = 0, max_bytes_for_external_set_bloom_filter = 27000, log_comment = '2 bits';
SELECT 'misses', count() FROM numbers(10000) WHERE cityHash64(number * 2 + 1) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_block_size = 100, max_bytes_for_external_set_cache = 0, max_bytes_for_external_set_bloom_filter = 12000, log_comment = 'budget too small';
SELECT 'misses', count() FROM numbers(10000) WHERE cityHash64(number * 2 + 1) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_block_size = 100, max_bytes_for_external_set_cache = 0, max_bytes_for_external_set_bloom_filter = 0, log_comment = 'no filter';

-- Each chunk of 100 lookups holds 5 keys that the set does not have, and each of them fills 2 runs of 10
-- rows, so the key repeats both right after itself and later in the chunk.
SELECT 'misses', count() FROM numbers(10000)
WHERE cityHash64((intDiv(number, 10) % 5 + intDiv(number, 100) * 5) * 2 + 1) IN (SELECT cityHash64(number * 2) FROM numbers(100000))
SETTINGS max_block_size = 100, max_bytes_for_external_set_cache = 0, log_comment = 'repeated misses';

-- Every set spilled.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

-- The lookups read fewer blocks with more bits per key.
WITH reads AS
(
    SELECT log_comment, ProfileEvents['ExternalSetReadBlocks'] AS blocks
    FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase()
)
SELECT
    (SELECT blocks FROM reads WHERE log_comment = 'filter') < (SELECT blocks FROM reads WHERE log_comment = '3 bits'),
    (SELECT blocks FROM reads WHERE log_comment = '3 bits') < (SELECT blocks FROM reads WHERE log_comment = '2 bits'),
    (SELECT blocks FROM reads WHERE log_comment = '2 bits') < (SELECT blocks FROM reads WHERE log_comment = 'no filter'),
    (SELECT blocks FROM reads WHERE log_comment = 'budget too small') = (SELECT blocks FROM reads WHERE log_comment = 'no filter');

-- Each chunk of 100 lookups searches for each of its keys once. The filters rule out at least 90% of these
-- keys with 8 bits per key, fewer of them with fewer bits per key, and none without filters.
WITH lookups AS
(
    SELECT log_comment, ProfileEvents['ExternalSetLookupKeys'] AS keys, ProfileEvents['ExternalSetBloomFilterSkippedKeys'] AS skipped
    FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase()
)
SELECT
    (SELECT groupUniqArray(keys) FROM lookups WHERE log_comment IN ('filter', '3 bits', '2 bits', 'budget too small', 'no filter')),
    (SELECT skipped >= 0.9 * keys FROM lookups WHERE log_comment = 'filter'),
    (SELECT skipped FROM lookups WHERE log_comment = 'filter') > (SELECT skipped FROM lookups WHERE log_comment = '3 bits'),
    (SELECT skipped FROM lookups WHERE log_comment = '3 bits') > (SELECT skipped FROM lookups WHERE log_comment = '2 bits'),
    (SELECT skipped FROM lookups WHERE log_comment = '2 bits') > 0,
    (SELECT max(skipped) FROM lookups WHERE log_comment IN ('budget too small', 'no filter')) = 0;

-- The events count the key of every row, repeated keys included, so the filters skip all 20 rows of each key
-- that they rule out.
SELECT
    ProfileEvents['ExternalSetLookupKeys'],
    ProfileEvents['ExternalSetBloomFilterSkippedKeys'] >= 0.9 * ProfileEvents['ExternalSetLookupKeys'],
    ProfileEvents['ExternalSetBloomFilterSkippedKeys'] % 20 = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'repeated misses';
SQL
