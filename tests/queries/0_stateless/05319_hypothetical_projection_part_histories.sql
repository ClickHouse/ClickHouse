-- parts can have different projection histories, so the span covers each part in each of its own layouts
SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, enable_parallel_replicas = 0, mutations_sync = 1;

DROP TABLE IF EXISTS history_est;
DROP TABLE IF EXISTS history_real;
CREATE TABLE history_est (p UInt8, a UInt64, b UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY a
    SETTINGS index_granularity = 150, index_granularity_bytes = '10Mi', use_const_adaptive_granularity = 0,
        min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, merge_max_block_size = 150;
CREATE TABLE history_real AS history_est;
ALTER TABLE history_real ADD PROJECTION p_b (SELECT p, a, b ORDER BY b) WITH SETTINGS (index_granularity = 100);

SYSTEM STOP MERGES history_est;
SYSTEM STOP MERGES history_real;
INSERT INTO history_est SELECT p, number, number + p * 50 FROM numbers(150) ARRAY JOIN [0, 1] AS p;
INSERT INTO history_est SELECT p, number + 150, number + 150 + p * 50 FROM numbers(150) ARRAY JOIN [0, 1] AS p;
INSERT INTO history_real SELECT p, number, number + p * 50 FROM numbers(150) ARRAY JOIN [0, 1] AS p;
INSERT INTO history_real SELECT p, number + 150, number + 150 + p * 50 FROM numbers(150) ARRAY JOIN [0, 1] AS p;
SYSTEM START MERGES history_est;
SYSTEM START MERGES history_real;
OPTIMIZE TABLE history_est FINAL;
OPTIMIZE TABLE history_real FINAL;
-- the projection of partition 1 now has the layout of a materialization, the one of partition 0 the layout of a merge
ALTER TABLE history_real CLEAR PROJECTION p_b IN PARTITION 1;
ALTER TABLE history_real MATERIALIZE PROJECTION p_b IN PARTITION 1;

CREATE HYPOTHETICAL PROJECTION p_b ON history_est (SELECT p, a, b ORDER BY b) WITH SETTINGS (index_granularity = 100);

SELECT '-- the estimate';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT p, a, b FROM history_est WHERE b = 250)
WHERE match(line, '^(marks_span|verdict):');

SELECT '-- the optimizer reads the base table';
SELECT countIf(explain LIKE '%ReadFromMergeTree (p_b)%') AS reads_projection
FROM (EXPLAIN indexes = 1 SELECT p, a, b FROM history_real WHERE b = 250);

DROP TABLE history_est;
DROP TABLE history_real;
