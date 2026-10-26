-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: the test drops the server-wide statistics cache and measures its size.
-- Tag no-parallel-replicas: the statistics are loaded on the initiator only.

-- The weight of a statistics cache entry is the memory retained by the statistics, not their serialized size:
-- a `uniq_v2` sketch of a column with a single value is serialized in a few bytes, but its aggregate function
-- state lives in an arena with a 4 KiB chunk. The `basic` statistics have no arena, but the entry retains the ASTs
-- of their descriptions: one in the description of the column statistics and one in the statistics object.

SET min_marks_per_part_to_use_statistics = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- The statistics are loaded to reorder the PREWHERE conditions.
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1;
SET automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS t_05394;

CREATE TABLE t_05394 (id UInt64 STATISTICS(uniq_v2), c UInt8 STATISTICS(uniq_v2))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '';

SYSTEM STOP MERGES t_05394;
INSERT INTO t_05394 SELECT number, 1 FROM numbers(10) SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;

SYSTEM DROP STATISTICS CACHE;

SELECT count() FROM t_05394 WHERE c = 1 AND id < 5 FORMAT Null;

-- 10 parts, 2 columns.
SELECT
    (SELECT value FROM system.metrics WHERE metric = 'StatisticsCacheCells') AS cells,
    (SELECT value FROM system.metrics WHERE metric = 'StatisticsCacheBytes') / cells >= 4096 AS weight_includes_arena;

DROP TABLE t_05394;

CREATE TABLE t_05394 (id UInt64 STATISTICS(basic), c UInt8 STATISTICS(basic))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '';

SYSTEM STOP MERGES t_05394;
INSERT INTO t_05394 SELECT number, 1 FROM numbers(10) SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;

SYSTEM DROP STATISTICS CACHE;

SELECT count() FROM t_05394 WHERE c = 1 AND id < 5 FORMAT Null;

-- Each of the two description ASTs is weighted at least 512 bytes.
WITH (SELECT value FROM system.metrics WHERE metric = 'StatisticsCacheCells') AS cells
SELECT
    cells > 0 AS loaded,
    (SELECT value FROM system.metrics WHERE metric = 'StatisticsCacheBytes') / cells >= 1024 AS weight_includes_descriptions;

DROP TABLE t_05394;
