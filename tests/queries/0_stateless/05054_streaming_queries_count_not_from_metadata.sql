-- Tags: no-parallel-replicas, no-shared-merge-tree

SET enable_analyzer = 1; -- streaming queries require the analyzer; the old-analyzer lane turns it off by default
SET enable_streaming_queries = 1;
SET use_skip_indexes_on_data_read = 0;

-- The metadata shortcut under test is the implicit min-max/count projection. CI randomizes both
-- of these, and the second is forced off when the first is off, so pin them: with the shortcut
-- disabled every query below reads through the reader and the test cannot observe the bug.
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t_stream_count;

CREATE TABLE t_stream_count (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

-- Pin one part per INSERT so the block numbers the cursor refers to are deterministic.
SYSTEM STOP MERGES t_stream_count;

INSERT INTO t_stream_count SELECT number, number * 10 FROM numbers(5);
INSERT INTO t_stream_count SELECT number, number * 10 FROM numbers(5, 5);
INSERT INTO t_stream_count SELECT number, number * 10 FROM numbers(10, 5);

-- `count()` and `countIf(1)` must agree: they count the same rows of the same read.
-- Only the bare `count()` is eligible for the metadata shortcut, so a disagreement means
-- the shortcut answered from part metadata and ignored the cursor.
SELECT 'cursor past every block, count()', count()    FROM t_stream_count STREAM BOUNDED CURSOR {'all': {'block_number': 1000000}};
SELECT 'cursor past every block, countIf(1)', countIf(1) FROM t_stream_count STREAM BOUNDED CURSOR {'all': {'block_number': 1000000}};

SELECT 'cursor at the last block, count()', count()    FROM t_stream_count STREAM BOUNDED CURSOR {'all': {'block_number': 3}};
SELECT 'cursor at the last block, countIf(1)', countIf(1) FROM t_stream_count STREAM BOUNDED CURSOR {'all': {'block_number': 3}};

SELECT 'no cursor, count()', count()    FROM t_stream_count STREAM BOUNDED;
SELECT 'no cursor, countIf(1)', countIf(1) FROM t_stream_count STREAM BOUNDED;

-- A non-streaming read still takes the shortcut and still returns the whole table.
SELECT 'not streaming, count()', count() FROM t_stream_count;

-- The streaming read must reach the reader rather than a metadata-derived source.
SELECT 'streaming count() reads through the reader', count() > 0
FROM (EXPLAIN SELECT count() FROM t_stream_count STREAM BOUNDED CURSOR {'all': {'block_number': 1000000}})
WHERE explain ILIKE '%ReadFromMergeTree%';

-- Control: the metadata shortcut under test is armed here. With `optimize_trivial_count_query` on, a
-- bare non-streaming `count()` is answered at plan-build time and no `ReadFromMergeTree` survives for
-- the projection pass, so the arm pins it off to reach the projection at all.
SELECT 'not streaming count() uses the min-max projection', count() > 0
FROM (EXPLAIN SELECT count() FROM t_stream_count SETTINGS optimize_trivial_count_query = 0)
WHERE explain ILIKE '%_minmax_count_projection%';

DROP TABLE t_stream_count;
