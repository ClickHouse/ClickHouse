#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Random settings limits: index_granularity=(8192, None); index_granularity_bytes=(33554432, None)
# - no-fasttest: uses object storage disks
# - no-parallel-replicas: other replicas read in their own read steps, with their own budgets

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `filesystem_prefetches_limit` is a maximum number of filesystem prefetches and
# `filesystem_prefetch_max_memory_usage` a maximum for the memory they hold. The Wide reader
# allocates one read buffer per prefetched substream, and one JSON column expands into many
# substreams, so both bounds have to cover the substreams of a whole read step rather than the
# columns of one reader: the readers of one step are alive at the same time.

# Both bounds are on the buffers alive at once, while ProfileEvents counts submissions, so an exact
# count is only assertable where no stream is destroyed mid-query. That holds for these plain
# Nullable columns; it does not hold for a JSON column, whose prefix streams are released after
# deserialization, so the JSON arms below are compared against each other.

# Every JSON arm reads ONE part with ONE thread, so its count is exact: the reader's substreams
# minus what the bound refuses. How many readers of a step are alive together is decided by thread
# scheduling and not by the query, so a count that needs several of them at once is not an oracle.
# The two readers that ARE alive together by construction are a task's PREWHERE one and its main one,
# and that is where one budget being shared across readers is asserted.

# A stream keeps its reservation while it lives, because consuming a prefetch moves the allocation
# into the read buffer rather than freeing it, so one reservation covers however many times that
# stream is prefetched. t_batches is the read that observes this across several batches; the wide
# and JSON fixtures read one granule each, and so prefetch every one of their streams once.

# The statements are grouped into four client invocations. Every prefetch bound below is a per-query
# setting, so grouping them costs nothing in coverage.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE t_wide40 ($(seq -f 'c%g Nullable(UInt64)' -s ', ' 1 40)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_w1/');

CREATE TABLE t_wide40_4parts ($(seq -f 'c%g Nullable(UInt64)' -s ', ' 1 40)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_w4/');

-- One part per reader and one granule per part, so every reader of the step is alive at once and
-- prefetches its substreams once: hence the pinned granularity, the 100-row inserts and STOP MERGES.
SYSTEM STOP MERGES t_wide40_4parts;

-- Plain UInt64, so one substream per column and every substream the same size: eight of them, three
-- granules of 8192 rows, and values a compression codec cannot shrink. A prefetch buffer smaller
-- than a granule's worth of data therefore leaves data pending after every batch, which is what
-- makes the reader prefetch the same streams again in the next one.
CREATE TABLE t_batches ($(seq -f 'c%g UInt64' -s ', ' 1 8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_b/');
SYSTEM STOP MERGES t_batches;

-- Packed part storage routes every file through ReadBufferFromFileView, which keeps the wrapped
-- buffer's state in itself between operations. On the default disk that buffer is an asynchronous
-- local-descriptor one, and the size it reports for a prefetch comes from exactly that state, so a
-- chain reading it without swapping the state back would charge zero bytes for a full-size buffer.
-- Pinned to a local disk: under an object-storage default policy both rows below measure zero.
CREATE TABLE t_packed ($(seq -f 'c%g UInt64' -s ', ' 1 8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         ratio_of_defaults_for_sparse_serialization = 1.0, storage_policy = 'default',
         min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
SYSTEM STOP MERGES t_packed;

CREATE TABLE t_json (jn Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_tj/');

-- The byte bound is measured on an encrypted disk on purpose: decryption wraps the buffer that owns
-- the prefetch allocation, so a chain that does not report its prefetch buffer size would read zero
-- bytes there and escape the bound entirely.
CREATE TABLE t_json_enc (jn Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 33554432,
         disk = disk(type = 'encrypted', key = '1234567812345678',
                     disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_enc/'));
"

# 40 Nullable columns are 80 prefetchable substreams (data plus null map per column), so one part
# already offers more substreams than the limit the reads below set and four parts offer four times
# as many.

# One JSON path per row, so as many distinct path types as rows: 20 of them, expanded
# Object -> Dynamic -> Variant into 22 prefetchable substreams. One part each, so one reader, whose
# substreams outnumber every JSON count bound set below.
json_value="toJSONString(map('a' || toString(number % 20),
        multiIf(number % 4 = 0, toString(number),
                number % 4 = 1, toString(number / 3),
                number % 4 = 2, toString(number % 7 = 0),
                toString(['x', 'y']))))"

# sipHash64 rather than `number`: a sequential column compresses into a single buffer whatever its row
# count, and would then be prefetched once no matter how many granules it spans.
batched_values=$(seq -f 'sipHash64(number, %g)' -s ', ' 1 8)

# One INSERT per part: separate statements in one invocation still write separate parts.
${CLICKHOUSE_CLIENT} -m --query "
INSERT INTO t_wide40 SELECT $(seq -f 'number + %g' -s ', ' 1 40) FROM numbers(100);
$(for _ in 1 2 3 4; do
    echo "INSERT INTO t_wide40_4parts SELECT $(seq -f 'number + %g' -s ', ' 1 40) FROM numbers(100);"
done)
INSERT INTO t_json SELECT $json_value FROM numbers(20);
INSERT INTO t_json_enc SELECT $json_value FROM numbers(20);
INSERT INTO t_batches SELECT $batched_values FROM numbers(24576);
INSERT INTO t_packed SELECT $batched_values FROM numbers(24576);
"

# The settings below intentionally override the values the test runner randomizes in: a statement's
# own SETTINGS clause wins over the session, and does not leak into the next statement. The page cache
# accounts no prefetch memory of its own and reading through the distributed cache replaces the buffer
# that owns the prefetch allocation, so both are pinned rather than left to the environment; the read
# pool is a per-query parameter here because the two pools reach the prefetch from different call
# paths.
# $1 - allow_prefetched_read_pool_for_remote_filesystem, $2 - filesystem_prefetches_limit,
# $3 - filesystem_prefetch_max_memory_usage, $4 - max_threads, $5 - log_comment suffix, $6 - query,
# $7... - extra settings for that one read
read_stmt() {
    local extra="${*:7}"
    echo "$6 SETTINGS log_comment = '04841_$5_${CLICKHOUSE_TEST_UNIQUE_NAME}',
        allow_prefetched_read_pool_for_remote_filesystem = $1,
        filesystem_prefetches_limit = $2, filesystem_prefetch_max_memory_usage = '$3',
        remote_filesystem_read_prefetch = 1, remote_filesystem_read_method = 'threadpool',
        max_threads = $4, merge_tree_prefetch_json_shared_data_substreams = 1,
        optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
        optimize_functions_to_subcolumns = 0, enable_filesystem_cache = 0,
        use_page_cache_for_disks_without_file_cache = 0,
        read_through_distributed_cache = 0${extra:+, $extra} FORMAT Null;"
}

# $1 - log_comment suffix -> that query's prefetch count, from the table built below
count_of() {
    echo "(SELECT prefetches FROM arms WHERE arm = '04841_$1_${CLICKHOUSE_TEST_UNIQUE_NAME}')"
}

# Local-descriptor prefetches increment no ProfileEvents counter, so the packed reads below are
# counted from the prefetches log instead, keyed on the same query.
# $1 - log_comment suffix -> that query's logged prefetch count
logged_count_of() {
    echo "(SELECT count() FROM system.filesystem_read_prefetches_log WHERE query_id =
              (SELECT query_id FROM arms WHERE arm = '04841_$1_${CLICKHOUSE_TEST_UNIQUE_NAME}'))"
}

wide_read="SELECT * FROM t_wide40"
wide4_read="SELECT * FROM t_wide40_4parts"
# One read task, two readers: the PREWHERE one over c1..c8 (16 substreams) and the main one over the
# other 32 columns (64). Both are created for the task and both live until it ends, so neither is
# subject to thread scheduling. Below the bound of 70 apiece, above it together, which is what tells
# one budget per read step apart from one per reader: per reader nothing is ever refused.
# PREWHERE is written out rather than moved there by the optimizer, which the reads here disable.
wide_prewhere_read="SELECT $(seq -f 'c%g' -s ', ' 9 40) FROM t_wide40
                    PREWHERE $(seq -f 'c%g >= 0' -s ' AND ' 1 8)"
json_read="SELECT count() FROM t_json WHERE length(JSONAllPaths(jn)) >= 0"
enc_read="SELECT count() FROM t_json_enc WHERE length(JSONAllPaths(jn)) >= 0"
# One part read by one thread, and a prefetch buffer well under a granule's worth of data so the
# reader still has data pending when the next batch starts.
batched_read="SELECT * FROM t_batches"
packed_read="SELECT * FROM t_packed"
# The packed reads are on the local disk pinned above, so their prefetches are local-descriptor ones:
# hence the local read method and the log rather than a ProfileEvents counter. The remote flag is off
# because the pin puts the part where only the local one is consulted.
packed_settings="local_filesystem_read_prefetch = 1, local_filesystem_read_method = 'pread_threadpool',
        remote_filesystem_read_prefetch = 0, enable_filesystem_read_prefetches_log = 1,
        max_read_buffer_size_local_fs = 4096"

${CLICKHOUSE_CLIENT} -m --query "
-- Off for the session so that the two arms that ask for it are the only ones the uncompressed cache
-- is in the buffer chain of: a statement's own SETTINGS clause wins over the session, and a value
-- repeated inside one clause does not.
SET use_uncompressed_cache = 0;
$(read_stmt 1 50 '1Gi'  4 'step_limit'       "$wide4_read")
$(read_stmt 1 0  '10Gi' 4 'step_unlimited'   "$wide4_read")
$(read_stmt 0 50 '1Gi'  4 'plain_pool_limit' "$wide_read")
$(read_stmt 0 0  '10Gi' 4 'plain_pool_unlim' "$wide_read")
$(read_stmt 1 70 '1Gi'  4 'shared_limit'     "$wide_prewhere_read")
$(read_stmt 1 0  '10Gi' 4 'shared_unlim'     "$wide_prewhere_read")
-- With the uncompressed cache in the chain the MergeTree data buffer is a cached one wrapping the
-- buffer that owns the prefetch allocation, so it too has to report that buffer's size.
$(read_stmt 1 0  '1'    4 'cache_bytes'      "$wide_read" 'use_uncompressed_cache = 1')
$(read_stmt 1 0  '10Gi' 4 'cache_unlim'      "$wide_read" 'use_uncompressed_cache = 1')
$(read_stmt 1 5  '1Gi'  1 'json_limit_5'     "$json_read")
$(read_stmt 1 15 '1Gi'  1 'json_limit_15'    "$json_read")
$(read_stmt 1 0  '10Gi' 1 'json_unlim'       "$json_read")
$(read_stmt 1 0  '1'    1 'json_enc_bytes'   "$enc_read")
-- Each row below that asserts nothing was prefetched has a companion read that differs only in the
-- byte bound, so that a fixture which stopped prefetching for some unrelated reason fails the pair
-- instead of passing the zero row.
$(read_stmt 1 0  '10Gi' 1 'json_enc_unlim'   "$enc_read")
$(read_stmt 0 4  '10Gi' 1 'batched_limit'    "$batched_read" 'max_read_buffer_size_remote_fs = 4096')
$(read_stmt 0 0  '10Gi' 1 'batched_unlim'    "$batched_read" 'max_read_buffer_size_remote_fs = 4096')
$(read_stmt 0 0  '1'    1 'packed_bytes'     "$packed_read" "$packed_settings")
$(read_stmt 0 0  '10Gi' 1 'packed_unlim'     "$packed_read" "$packed_settings")
SYSTEM FLUSH LOGS query_log, filesystem_read_prefetches_log;
"

# The readers of one step are created as threads pick their tasks up, so two of them can charge the
# budget, finish, release it and let the next two charge it again. The cumulative submissions are then
# a multiple of the limit rather than the limit itself, which is why the first row is an inequality:
# what the bound promises is that no set of readers alive together exceeds it.

# The pool that prefetches on admission is off for the plain-pool pair, so those reads go through the
# reader's own path instead. One part on purpose: readers there are created as threads pick tasks up,
# so with several parts one pair can charge the budget, finish, release it and let the next pair
# charge it again, which makes the cumulative count a multiple of the limit rather than the limit.

# The three JSON arms are one reader over one part at three bounds. Releasing a prefix stream returns
# its capacity to the budget, so a bounded arm submits its limit plus that churn: the DIFFERENCE of
# the two bounded counts is the difference of their limits whatever the churn is, while a budget that
# did not reach JSON substreams at all would leave all three counts equal.

# Four of the eight streams get a reservation and keep it, so the bounded batched read submits four
# prefetches per batch where the unbounded one submits eight: the two counts divided by 4 and by 8 are
# the same batch count, cross-multiplied to stay integral. A saturated budget that stopped the reader
# after the first batch would leave the bounded count at 4 whatever the second count is. How many
# batches a granule's data takes is not fixed (compression block sizes are randomized), which is why
# neither count is compared against a literal; the following row pins that there was more than one
# batch, since 8 would mean the fixture collapsed into one and both counts would agree for the wrong
# reason.

# One scan of query_log for the whole assertion block: the arms are keyed by their log_comment, and
# the latest run of each wins, so a retried query cannot contribute a stale count.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TEMPORARY TABLE arms AS
    SELECT log_comment AS arm,
           argMax(ProfileEvents['RemoteFSPrefetches'], event_time_microseconds) AS prefetches,
           argMax(ProfileEvents['RowsReadByPrewhereReaders'], event_time_microseconds) AS prewhere_rows,
           argMax(query_id, event_time_microseconds) AS query_id
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND event_date >= yesterday() AND is_initial_query
      AND log_comment LIKE '04841\_%\_${CLICKHOUSE_TEST_UNIQUE_NAME}'
    GROUP BY arm;

SELECT 'the step never reaches what its four readers asked for',
       $(count_of step_limit) < $(count_of step_unlimited);
SELECT 'the limit is observed, not naturally small: four readers want 4 x 80 of them',
       $(count_of step_unlimited) = 320;
SELECT 'a single reader above the limit prefetches the first N substreams, not none',
       $(count_of plain_pool_limit) = 50;
SELECT 'and all 80 of them when unlimited',
       $(count_of plain_pool_unlim) = 80;
SELECT 'the two readers of one task really are two',
       (SELECT prewhere_rows FROM arms WHERE arm = '04841_shared_limit_${CLICKHOUSE_TEST_UNIQUE_NAME}') > 0;
SELECT 'and between them they ask for all 80 substreams',
       $(count_of shared_unlim) = 80;
SELECT 'one budget for the task, not one per reader: neither reader alone reaches the bound',
       $(count_of shared_limit) < $(count_of shared_unlim);
SELECT 'a bounded JSON read prefetches fewer substreams than an unbounded one',
       $(count_of json_limit_5) < $(count_of json_unlim);
SELECT 'and the bound is what decides how many: limits 10 apart, counts 10 apart',
       $(count_of json_limit_15) - $(count_of json_limit_5) = 10;
SELECT 'a stream that ends returns its capacity: the arm submits more than it may hold',
       $(count_of json_limit_5) > 5;
SELECT 'prefetching does happen on this fixture when the byte bound is not the binding one',
       $(count_of json_enc_unlim) > 0;
SELECT 'the memory bound alone stops prefetching, on an encrypted disk',
       $(count_of json_enc_bytes) = 0;
SELECT 'prefetching does happen through the uncompressed cache when the bound is not binding',
       $(count_of cache_unlim) > 0;
SELECT 'and the memory bound stops prefetching there too, through the uncompressed cache',
       $(count_of cache_bytes) = 0;
SELECT 'a reserved stream may prefetch again in a later batch, without new memory',
       $(count_of batched_limit) * 8 = $(count_of batched_unlim) * 4;
SELECT 'and it took more than one batch: unbounded, all eight streams prefetch in each of them',
       $(count_of batched_unlim) > 8;
SELECT 'the fixture is packed part storage',
       (SELECT any(part_storage_type) = 'Packed' FROM system.parts
        WHERE database = currentDatabase() AND table = 't_packed' AND active);
SELECT 'prefetching does happen through the file view when the byte bound is not the binding one',
       $(logged_count_of packed_unlim) > 0;
SELECT 'and the memory bound stops prefetching there too, through the file view',
       $(logged_count_of packed_bytes) = 0;

DROP TABLE t_wide40; DROP TABLE t_wide40_4parts; DROP TABLE t_json; DROP TABLE t_json_enc;
DROP TABLE t_batches; DROP TABLE t_packed;
"
