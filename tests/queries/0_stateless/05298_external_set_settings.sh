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

# Exact memory tracking makes a threshold of 1 byte spill every set to disk before its first chunk, and the
# sorter write each chunk as a run. A query that fails does not stop the others; the report below tells how
# each one ended.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 --max_untracked_memory 0 \
    --ignore-error --multiquery 2> "${LOCAL_DIR}/errors.log" <<'SQL'
-- A set on disk counts its temporary files and their bytes, and the query names the set among the
-- operators that wrote temporary files. Without a threshold, nothing is written.
SELECT 'metrics', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000000))
SETTINGS max_bytes_before_external_set = '4M', log_comment = 'metrics';
SELECT 'disabled', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000000))
SETTINGS max_bytes_before_external_set = 0, log_comment = 'disabled';

-- A subquery read by many threads fills the set as one stream, with the same result in memory and on disk.
SELECT 'threads', count(), sum(cityHash64(number)) FROM numbers_mt(1000000)
WHERE number % 300000 IN (SELECT number * 7 % 300000 FROM numbers_mt(1000000))
SETTINGS max_bytes_before_external_set = 0, max_threads = 8, log_comment = 'threads in memory';
SELECT 'threads', count(), sum(cityHash64(number)) FROM numbers_mt(1000000)
WHERE number % 300000 IN (SELECT number * 7 % 300000 FROM numbers_mt(1000000))
SETTINGS max_bytes_before_external_set = 1, max_threads = 8, log_comment = 'threads on disk';

-- A positive ratio enables spilling even when it gives less than one byte, alone or with the
-- absolute threshold.
SELECT 'ratio', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000))
SETTINGS max_memory_usage_for_user = 536870912, max_bytes_ratio_before_external_set = 1e-18, log_comment = 'tiny ratio';
SELECT 'ratio', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000))
SETTINGS max_memory_usage_for_user = 536870912, max_bytes_before_external_set = 1, max_bytes_ratio_before_external_set = 1e-18,
    log_comment = 'tiny ratio and threshold';

-- `max_rows_in_set` counts the distinct keys of the whole set, in memory and on disk alike:
-- reaching the limit is allowed, exceeding it throws.
SELECT 'rows limit', count() FROM numbers(200) WHERE number IN (SELECT number % 100 FROM numbers(1000))
SETTINGS max_rows_in_set = 100, max_bytes_before_external_set = 0, log_comment = 'rows limit reached in memory';
SELECT 'rows limit', count() FROM numbers(200) WHERE number IN (SELECT number % 100 FROM numbers(1000))
SETTINGS max_rows_in_set = 100, max_bytes_before_external_set = 1, max_block_size = 50, log_comment = 'rows limit reached on disk';
SELECT 'rows limit', count() FROM numbers(200) WHERE number IN (SELECT number % 101 FROM numbers(1000))
SETTINGS max_rows_in_set = 100, max_bytes_before_external_set = 0, log_comment = 'rows limit exceeded in memory';
SELECT 'rows limit', count() FROM numbers(200) WHERE number IN (SELECT number % 101 FROM numbers(1000))
SETTINGS max_rows_in_set = 100, max_bytes_before_external_set = 1, max_block_size = 50, log_comment = 'rows limit exceeded on disk';

-- `max_bytes_in_set` limits the memory of the set. Once on disk, that memory is the directory of
-- its file, so a set that exceeds the limit in memory fits on disk.
SELECT 'bytes limit', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000))
SETTINGS max_bytes_in_set = 1048576, max_bytes_before_external_set = 0, log_comment = 'bytes limit in memory';
SELECT 'bytes limit', count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000))
SETTINGS max_bytes_in_set = 1048576, max_bytes_before_external_set = 1, log_comment = 'bytes limit on disk';

-- In the `break` overflow mode, a set in memory keeps the keys of the blocks up to the one that
-- reaches the limits and stops reading the subquery. A set on disk counts its distinct keys when it
-- merges them into its file: it reads the whole subquery and keeps the lowest keys, up to the
-- merged block that reaches the limits. The keys arrive in an order that differs from theirs, so
-- the set in memory keeps other keys than the keys below 100 that the set on disk keeps.
SELECT 'rows break', count(), countIf(number < 100) FROM numbers(20000)
WHERE number IN (SELECT number * 7919 % 10000 FROM numbers(20000))
SETTINGS max_rows_in_set = 100, set_overflow_mode = 'break', max_block_size = 50, max_bytes_before_external_set = 0,
    log_comment = 'rows break in memory';
SELECT 'rows break', count(), countIf(number < 100) FROM numbers(20000)
WHERE number IN (SELECT number * 7919 % 10000 FROM numbers(20000))
SETTINGS max_rows_in_set = 100, set_overflow_mode = 'break', max_block_size = 50, max_bytes_before_external_set = 1,
    log_comment = 'rows break on disk';

-- The set in memory reaches the byte limit with its first block, and the set on disk with the
-- directory of its first merged block.
SELECT 'bytes break', count() FROM numbers(200000) WHERE number IN (SELECT number FROM numbers(200000))
SETTINGS max_bytes_in_set = 1024, set_overflow_mode = 'break', max_bytes_before_external_set = 0, log_comment = 'bytes break in memory';
SELECT 'bytes break', count() FROM numbers(200000) WHERE number IN (SELECT number FROM numbers(200000))
SETTINGS max_bytes_in_set = 1024, set_overflow_mode = 'break', max_bytes_before_external_set = 1, log_comment = 'bytes break on disk';

-- The subquery reads every partition through its own stream, and the preliminary `DISTINCT` of each stream
-- passes the keys through under the threshold of the set, which then removes all the repeats on disk.
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

-- The report shows, for each query, how it ended, the operators that wrote temporary files, the
-- sets that spilled to disk, whether they wrote temporary files, how many times their runs were
-- merged, and whether the lookups read them from disk.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, if(type = 'QueryFinish', 'finished', errorCodeToName(exception_code)), spilled_to_disk,
    ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetWritePart'] > 0, ProfileEvents['ExternalSetMerge'],
    ProfileEvents['ExternalSetReadBlocks'] > 0
FROM system.query_log
WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing') AND log_comment != ''
ORDER BY event_time_microseconds;

-- The set on disk counts every temporary file in the events of external processing, and its runs
-- are merged once.
SELECT 'metrics', ProfileEvents['ExternalSetWritePart'] >= 2, ProfileEvents['ExternalSetMerge'] = 1,
    ProfileEvents['ExternalSetCompressedBytes'] >= 100000, ProfileEvents['ExternalSetUncompressedBytes'] >= 100000,
    ProfileEvents['ExternalProcessingFilesTotal'] = ProfileEvents['ExternalSetWritePart'], ProfileEvents['ExternalSortMerge'] = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = 'metrics';

-- Every set that reaches its limits in the `break` mode counts an overflow. Only the sets on disk
-- read the whole subquery along with the left side.
SELECT log_comment, ProfileEvents['OverflowBreak'] > 0, read_rows = if(startsWith(log_comment, 'rows'), 40000, 400000)
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment LIKE '% break %'
ORDER BY event_time_microseconds;

-- Every preliminary `DISTINCT` passes the keys through once the set spills to disk, and none does in memory.
SELECT log_comment, ProfileEvents['DistinctTransformsSwitchedToPassThrough']
FROM system.query_log
WHERE type = 'QueryFinish' AND startsWith(log_comment, 'preliminary distinct')
ORDER BY event_time_microseconds;
SQL

# The queries that exceed a limit fail with it.
grep -o -E 'Limit for IN-set exceeded, max (rows|bytes)' "${LOCAL_DIR}/errors.log"
