#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-settings.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# With exact memory tracking, a threshold of 1 byte spills every set before its first chunk and writes each chunk
# as a run. Failing queries do not stop the others, and the report tells how each one ended.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 --max_untracked_memory 0 \
    --ignore-error --multiquery 2> "${LOCAL_DIR}/errors.log" <<'SQL'
-- A set on disk counts its temporary files and bytes and appears in `spilled_to_disk`; without a threshold,
-- nothing is written.
SELECT 'metrics', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000000))
SETTINGS max_bytes_before_external_set = '4M', log_comment = 'metrics';
SELECT 'disabled', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000000))
SETTINGS max_bytes_before_external_set = 0, log_comment = 'disabled';

-- A subquery read by many threads fills the set as one stream.
SELECT 'threads', count(), sum(cityHash64(number)) FROM numbers_mt(1000000)
WHERE number % 300000 IN (SELECT number * 7 % 300000 FROM numbers_mt(1000000))
SETTINGS max_bytes_before_external_set = 0, max_threads = 8, log_comment = 'threads in memory';
SELECT 'threads', count(), sum(cityHash64(number)) FROM numbers_mt(1000000)
WHERE number % 300000 IN (SELECT number * 7 % 300000 FROM numbers_mt(1000000))
SETTINGS max_bytes_before_external_set = 1, max_threads = 8, log_comment = 'threads on disk';

-- A positive ratio enables spilling even below one byte, alone or with the absolute threshold.
SELECT 'ratio', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000))
SETTINGS max_memory_usage_for_user = 536870912, max_bytes_ratio_before_external_set = 1e-18, log_comment = 'tiny ratio';
SELECT 'ratio', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000))
SETTINGS max_memory_usage_for_user = 536870912, max_bytes_before_external_set = 1, max_bytes_ratio_before_external_set = 1e-18,
    log_comment = 'tiny ratio and threshold';

-- `max_rows_in_set` counts distinct keys in memory and on disk: reaching it passes, exceeding it throws.
SELECT 'rows limit', count() FROM numbers(200) WHERE number IN (SELECT number % 100 FROM numbers(1000))
SETTINGS max_rows_in_set = 100, max_bytes_before_external_set = 0, log_comment = 'rows limit reached in memory';
SELECT 'rows limit', count() FROM numbers(200) WHERE number IN (SELECT number % 100 FROM numbers(1000))
SETTINGS max_rows_in_set = 100, max_bytes_before_external_set = 1, max_block_size = 50, log_comment = 'rows limit reached on disk';
SELECT 'rows limit', count() FROM numbers(200) WHERE number IN (SELECT number % 101 FROM numbers(1000))
SETTINGS max_rows_in_set = 100, max_bytes_before_external_set = 0, log_comment = 'rows limit exceeded in memory';
SELECT 'rows limit', count() FROM numbers(200) WHERE number IN (SELECT number % 101 FROM numbers(1000))
SETTINGS max_rows_in_set = 100, max_bytes_before_external_set = 1, max_block_size = 50, log_comment = 'rows limit exceeded on disk';

-- `max_bytes_in_set` limits the memory of the set, which on disk is only its directory.
SELECT 'bytes limit', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000))
SETTINGS max_bytes_in_set = 1048576, max_bytes_before_external_set = 0, log_comment = 'bytes limit in memory';
SELECT 'bytes limit', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000))
SETTINGS max_bytes_in_set = 1048576, max_bytes_before_external_set = 1, log_comment = 'bytes limit on disk';

-- In the `break` mode, a set in memory keeps the blocks up to the one that reaches the limit and stops reading.
-- A set on disk reads the whole subquery and keeps the keys that come first in the order of its disk keys, up
-- to the merged block that reaches the limit: here the keys below 100, which arrive scattered.
SELECT 'rows break', count(), countIf(number < 100) FROM numbers(20000)
WHERE number IN (SELECT number * 7919 % 10000 FROM numbers(20000))
SETTINGS max_rows_in_set = 100, set_overflow_mode = 'break', max_block_size = 50, max_bytes_before_external_set = 0,
    log_comment = 'rows break in memory';
SELECT 'rows break', count(), countIf(number < 100) FROM numbers(20000)
WHERE number IN (SELECT number * 7919 % 10000 FROM numbers(20000))
SETTINGS max_rows_in_set = 100, set_overflow_mode = 'break', max_block_size = 50, max_bytes_before_external_set = 1,
    log_comment = 'rows break on disk';

-- The set in memory reaches the byte limit with its first block, and the set on disk with its first merged block.
SELECT 'bytes break', count() FROM numbers(200000) WHERE number IN (SELECT number FROM numbers(200000))
SETTINGS max_bytes_in_set = 1024, set_overflow_mode = 'break', max_bytes_before_external_set = 0, log_comment = 'bytes break in memory';
SELECT 'bytes break', count() FROM numbers(200000) WHERE number IN (SELECT number FROM numbers(200000))
SETTINGS max_bytes_in_set = 1024, set_overflow_mode = 'break', max_bytes_before_external_set = 1, log_comment = 'bytes break on disk';

-- Each partition is read by its own stream, whose preliminary `DISTINCT` passes the keys through under the
-- threshold of the set; the set on disk removes the repeats.
CREATE TABLE partitioned (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8
SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO partitioned SELECT number % 200000, number FROM numbers_mt(800000);
SELECT 'preliminary distinct', count() FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(300000) WHERE number IN (SELECT a FROM partitioned)
    SETTINGS allow_creating_set_partitions_independently = 1, max_threads = 8) WHERE explain LIKE '%Pre-distinct: 1%';
SELECT 'preliminary distinct', count(), sum(cityHash64(number)) FROM numbers(300000) WHERE number IN (SELECT a FROM partitioned)
SETTINGS allow_creating_set_partitions_independently = 1, max_threads = 8, max_block_size = 6540, max_bytes_before_external_set = 0,
    log_comment = 'preliminary distinct in memory';
SELECT 'preliminary distinct', count(), sum(cityHash64(number)) FROM numbers(300000) WHERE number IN (SELECT a FROM partitioned)
SETTINGS allow_creating_set_partitions_independently = 1, max_threads = 8, max_block_size = 6540, max_bytes_before_external_set = 1,
    log_comment = 'preliminary distinct on disk';

-- For each query: how it ended, `spilled_to_disk`, the sets spilled, whether they wrote files, the merges of
-- runs, and whether the lookups read the disk.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, if(type = 'QueryFinish', 'finished', errorCodeToName(exception_code)), spilled_to_disk,
    ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetWritePart'] > 0, ProfileEvents['ExternalSetMerge'],
    ProfileEvents['ExternalSetReadBlocks'] > 0
FROM system.query_log
WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing') AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

-- The temporary files of the set count in the events of external processing, and its runs merge once.
SELECT 'metrics', ProfileEvents['ExternalSetWritePart'] >= 2, ProfileEvents['ExternalSetMerge'] = 1,
    ProfileEvents['ExternalSetCompressedBytes'] >= 100000, ProfileEvents['ExternalSetUncompressedBytes'] >= 100000,
    ProfileEvents['ExternalProcessingFilesTotal'] = ProfileEvents['ExternalSetWritePart'], ProfileEvents['ExternalSortMerge'] = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = 'metrics' AND current_database = currentDatabase();

-- Every set that reaches its limits in the `break` mode counts an overflow; only sets on disk read the whole
-- subquery.
SELECT log_comment, ProfileEvents['OverflowBreak'] > 0, read_rows = if(startsWith(log_comment, 'rows'), 40000, 400000)
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment LIKE '% break %' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

-- Every preliminary `DISTINCT` passes the keys through once the set spills, and none does in memory.
SELECT log_comment, ProfileEvents['DistinctTransformsSwitchedToPassThrough']
FROM system.query_log
WHERE type = 'QueryFinish' AND startsWith(log_comment, 'preliminary distinct') AND current_database = currentDatabase()
ORDER BY event_time_microseconds;
SQL

# The queries over a limit fail with it.
grep -o -E 'Limit for IN-set exceeded, max (rows|bytes)' "${LOCAL_DIR}/errors.log"
