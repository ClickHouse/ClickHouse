-- Tags: no-parallel-replicas
-- no-parallel-replicas: STREAM is not supported with parallel replicas.

-- A STREAM read over a text-indexed table aborted with a LOGICAL_ERROR from the snapshot
-- sub-plan being optimized twice (see the fix commit for the mechanism).

SET enable_streaming_queries = 1;
SET allow_experimental_full_text_index = 1;
-- The double-optimize only corrupts the plan when the direct-read-from-text-index optimization
-- fires. CI randomizes both settings below; with either off the query takes the row-scan path,
-- never reaches the faulty rewrite, and the test would pass even on the unfixed build. Pin them.
SET query_plan_direct_read_from_text_index = 1;
-- Direct read replaces the text predicate in the read's immediate parent filter; the streaming
-- source builds separate cursor and PREWHERE filters, so the predicate only lands directly above
-- the read once the two filters are merged (query_plan_merge_filters, randomized by CI).
SET query_plan_merge_filters = 1;

DROP TABLE IF EXISTS t_stream_text_index;

CREATE TABLE t_stream_text_index
(
    id UInt64,
    map Map(String, String),
    INDEX idx_map_keys mapKeys(map) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX idx_map_values mapValues(map) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_stream_text_index
SELECT number, map('service', if(number < 5, 'web-api', 'backend'), 'env', if(number < 3, 'prod', 'staging'))
FROM numbers(20);

-- Guard that direct read from the text index is actually active for this predicate (otherwise the
-- STREAM query below never exercises the buggy path and the test silently stops covering the fix):
-- with the optimization on, the read step gains a __text_index_* virtual column.
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM t_stream_text_index WHERE ('prod') IN (map[materialize('env')]))
WHERE position(explain, '__text_index_') > 0;

-- Must build the streaming pipeline without aborting and return the matching rows (id 0, 1, 2).
-- The inner LIMIT bounds the STREAM to the initial snapshot so the query completes; the outer
-- ORDER BY sorts the finite result deterministically (a STREAM read is in commit order).
SELECT id FROM (SELECT id FROM t_stream_text_index STREAM PREWHERE ('prod') IN (map[materialize('env')]) LIMIT 3) ORDER BY id;

DROP TABLE t_stream_text_index;
