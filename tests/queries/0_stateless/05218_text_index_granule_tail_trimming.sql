-- Tags: no-parallel-replicas
-- no-parallel-replicas: with `parallel_replicas_local_plan = 0` the initiator reads no rows
-- itself, so the RowsReadByMainReader assertion below reads 0 instead of 2.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_text_index_tail_trim;

CREATE TABLE t_text_index_tail_trim
(
    id UInt64,
    message String,
    payload String,
    INDEX idx message TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
-- Wide parts only: a Compact part's reader cannot serve incomplete granules, so the assertions
-- below would fail there rather than pass.
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_text_index_tail_trim
SELECT
    number,
    concat('common w', toString(number % 977), if(number % 8192 = 0 AND intDiv(number, 8192) % 2 = 0, ' needle', '')),
    repeat('x', 512)
FROM numbers(32768);

OPTIMIZE TABLE t_text_index_tail_trim FINAL;

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_text_index_tail_trim' AND active;

-- 2 matching rows, each the first row of its granule, in 2 of the 4 granules.
SELECT count() FROM t_text_index_tail_trim WHERE hasToken(message, 'needle');

-- The rows below are only read through the text index if the plan reads that index on the data path,
-- which it does by producing a `__text_index_*` virtual column.
SELECT count() > 0 FROM (
    EXPLAIN SELECT sum(id), sum(length(payload)) FROM t_text_index_tail_trim
    WHERE hasToken(message, 'needle')
    SETTINGS query_plan_direct_read_from_text_index = 1, optimize_move_to_prewhere = 1,
        query_plan_optimize_prewhere = 1
) WHERE explain ILIKE '%__text_index_%';

-- `sum(id)` pins WHICH rows are returned, not just how many.
-- Both prewhere settings have to be on for the text condition to reach the readers chain as a filter,
-- and either one off disables it.
SELECT sum(id), sum(length(payload)) FROM t_text_index_tail_trim WHERE hasToken(message, 'needle')
SETTINGS log_comment = '05218_selective', query_plan_direct_read_from_text_index = 1,
    optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    use_query_condition_cache = 0, max_threads = 1, max_block_size = 8192;

-- max_block_size is not a multiple of index_granularity, so a read stops inside a mark and the next
-- one continues there. Both a selective and a match-everything predicate must survive that.
SELECT sum(id), sum(length(payload)) FROM t_text_index_tail_trim WHERE hasToken(message, 'needle')
SETTINGS log_comment = '05218_selective_midmark', query_plan_direct_read_from_text_index = 1,
    optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    use_query_condition_cache = 0, max_threads = 1, max_block_size = 3000;

SELECT sum(length(payload)) FROM t_text_index_tail_trim WHERE hasToken(message, 'common')
SETTINGS query_plan_direct_read_from_text_index = 1, use_query_condition_cache = 0,
    max_threads = 1, max_block_size = 3000;

SYSTEM FLUSH LOGS query_log;

-- `payload` is read by the main reader, since the filter is on `message` alone. Without granule-tail
-- trimming it is read for all 8192 rows of each selected granule instead of for the matching row.
-- The counter is only meaningful if the text index reader is the one in the chain; the third
-- column is nonzero only when MergeTreeReaderTextIndex::readRows ran.
SELECT log_comment,
    argMax(ProfileEvents['RowsReadByMainReader'], event_time_microseconds),
    argMax(ProfileEvents['TextIndexReaderTotalMicroseconds'], event_time_microseconds) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05218_selective', '05218_selective_midmark')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_text_index_tail_trim;
