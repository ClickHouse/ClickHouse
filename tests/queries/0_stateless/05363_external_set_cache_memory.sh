#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-cache-memory.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# A lookup call adds the blocks that it reads from the file to the cache of the set in batches, so the copies
# of these blocks that wait for the cache take a few MiB however many blocks the call reads. The set holds
# 4,000,000 tuples of four 64-bit hashes in about 31,000 blocks, and one call of 65,536 lookups reads about
# 20,000 of them into a cache of 1 MiB. With one thread and exact memory tracking, the peak memory does not
# depend on scheduling. The test prints the result of each query, whether its set spilled and whether the call
# read more than 10,000 blocks, and whether the peak memory with the cache stays within 16 MiB of the peak
# without it.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 --max_untracked_memory 0 \
    --max_threads 1 --max_block_size 65536 --max_bytes_before_external_set 16777216 --max_bytes_ratio_before_external_set 0 \
    --multiquery <<'SQL'
SELECT 'cache', count() FROM numbers(65536)
WHERE (cityHash64(number), cityHash64(number + 1), cityHash64(number + 2), cityHash64(number + 3))
    IN (SELECT cityHash64(number * 2), cityHash64(number * 2 + 1), cityHash64(number * 2 + 2), cityHash64(number * 2 + 3) FROM numbers(4000000))
SETTINGS max_bytes_for_external_set_cache = 1048576, log_comment = 'cache';
SELECT 'no cache', count() FROM numbers(65536)
WHERE (cityHash64(number), cityHash64(number + 1), cityHash64(number + 2), cityHash64(number + 3))
    IN (SELECT cityHash64(number * 2), cityHash64(number * 2 + 1), cityHash64(number * 2 + 2), cityHash64(number * 2 + 3) FROM numbers(4000000))
SETTINGS max_bytes_for_external_set_cache = 0, log_comment = 'no cache';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetReadBlocks'] > 10000
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

WITH peaks AS
(
    SELECT log_comment, memory_usage
    FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase()
)
SELECT (SELECT memory_usage FROM peaks WHERE log_comment = 'cache') < (SELECT memory_usage FROM peaks WHERE log_comment = 'no cache') + 16777216;
SQL
