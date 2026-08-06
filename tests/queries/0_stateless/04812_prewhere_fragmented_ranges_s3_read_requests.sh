#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-distributed-cache
# no-random-settings, no-random-merge-tree-settings: the test asserts the number of S3 read
# requests, which depends on buffer sizes, compression block sizes and the reading pool.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A skip index applied at index analysis time fragments the mark ranges of a read task, and
# with PREWHERE the second column is read by a later step of the readers chain over the
# fragmented ranges. The right bound of ranged read requests of that step has to span the
# whole task: a bound narrowed to the ranges started by each read advances on every read,
# and each advance drops the in-flight read request of the buffer and issues a new small one,
# which multiplies S3 read requests.

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t_prewhere_s3_requests;

CREATE TABLE t_prewhere_s3_requests
(
    id UInt64,
    v UInt64,
    b UInt64,
    INDEX ix_v v TYPE set(8) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0, index_granularity = 16,
    min_compress_block_size = 512, max_compress_block_size = 512;

-- v is constant within each 16-row granule; only every 10th granule matches v = 3.
INSERT INTO t_prewhere_s3_requests SELECT number, intDiv(number, 16) % 10, number FROM numbers(60000);
"

query_id="04812_prewhere_s3_requests_${CLICKHOUSE_DATABASE}_$RANDOM"

# The skip index is applied at analysis time and must fragment the ranges before the read.
${CLICKHOUSE_CLIENT} --query "
EXPLAIN indexes = 1
SELECT sum(b) FROM t_prewhere_s3_requests PREWHERE v = 3
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, enable_parallel_replicas = 0
" | grep -A4 'Name: ix_v' | grep 'Granules:' | sed 's/^ *//'

${CLICKHOUSE_CLIENT} --query_id "$query_id" -m --query "
SELECT sum(b) FROM t_prewhere_s3_requests
PREWHERE v = 3
SETTINGS
    use_skip_indexes = 1,
    use_skip_indexes_on_data_read = 0,
    merge_tree_min_rows_for_seek = 0,
    merge_tree_min_bytes_for_seek = 0,
    max_rows_to_read = 0,
    enable_parallel_replicas = 0,
    max_threads = 1,
    max_block_size = 64,
    merge_tree_min_rows_for_concurrent_read = 1000000000,
    merge_tree_min_bytes_for_concurrent_read = 1000000000,
    allow_prefetched_read_pool_for_remote_filesystem = 0,
    use_page_cache_for_disks_without_file_cache = 0,
    enable_filesystem_cache = 0,
    use_query_condition_cache = 0,
    remote_read_min_bytes_for_seek = 4194304;
"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# The part has ~3750 granules, ~375 of them survive the index analysis. Without a task-wide
# right bound of read requests this query issues about a hundred requests (one per bound
# advance) for the column read by the step after PREWHERE, versus a few with the bound.
${CLICKHOUSE_CLIENT} -m --query "
SELECT
    ProfileEvents['S3ReadRequestsCount'] < 20 AS few_read_requests
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query_id = '$query_id';
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_prewhere_s3_requests"
