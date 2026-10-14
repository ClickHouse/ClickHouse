-- the writer sizes the granules of a projection part with the columns of its key expression, also when the part does not store them
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0, allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_est;
DROP TABLE IF EXISTS t_real;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, b, v ORDER BY intDiv(b, 10));

INSERT INTO t_est SELECT number, number % 100, number FROM numbers(3000);
INSERT INTO t_real SELECT number, number % 100, number FROM numbers(3000);

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, b, v ORDER BY intDiv(b, 10));
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE intDiv(b, 10) = 4)
WHERE match(line, '^(Baseline|With |marks:|status:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real WHERE intDiv(b, 10) = 4)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');


-- a row of a LowCardinality column stores only its index, for a key and for a stored column
DROP HYPOTHETICAL PROJECTION p_k ON t_est;
CREATE TABLE t_real_lc AS t_est;
ALTER TABLE t_real_lc ADD PROJECTION p_lc (SELECT a, b, v ORDER BY toLowCardinality(intDiv(b, 10)));
INSERT INTO t_real_lc SELECT number, number % 100, number FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_lc ON t_est (SELECT a, b, v ORDER BY toLowCardinality(intDiv(b, 10)));
SELECT '-- a key that returns LowCardinality';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE toLowCardinality(intDiv(b, 10)) < 5)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real_lc WHERE toLowCardinality(intDiv(b, 10)) < 5)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

-- a LowCardinality column inside an array has a dictionary too
DROP HYPOTHETICAL PROJECTION p_lc ON t_est;
CREATE TABLE t_real_arr AS t_est;
ALTER TABLE t_real_arr ADD PROJECTION p_arr (SELECT a, b, v ORDER BY [toLowCardinality(intDiv(b, 10))]);
INSERT INTO t_real_arr SELECT number, number % 100, number FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_arr ON t_est (SELECT a, b, v ORDER BY [toLowCardinality(intDiv(b, 10))]);
SELECT '-- a key with LowCardinality inside an array';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE [toLowCardinality(intDiv(b, 10))] < [7])
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real_arr WHERE [toLowCardinality(intDiv(b, 10))] < [7])
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

CREATE TABLE s_est (a UInt64, b UInt64, c LowCardinality(String)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0;
CREATE TABLE s_real AS s_est;
ALTER TABLE s_real ADD PROJECTION p_b (SELECT a, b, c ORDER BY b);
INSERT INTO s_est SELECT number, number % 100, repeat(toString(number % 5), 60) FROM numbers(3000);
INSERT INTO s_real SELECT number, number % 100, repeat(toString(number % 5), 60) FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_b ON s_est (SELECT a, b, c ORDER BY b);
SELECT '-- a stored LowCardinality(String) column';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, c FROM s_est WHERE b < 50)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, c FROM s_real WHERE b < 50)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

-- a stored array of LowCardinality values, the read gives the array a shared dictionary
CREATE TABLE sa_est (a UInt64, b UInt64, c Array(LowCardinality(String))) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0;
CREATE TABLE sa_real AS sa_est;
ALTER TABLE sa_real ADD PROJECTION p_sa (SELECT a, b, c ORDER BY b);
INSERT INTO sa_est SELECT number, number % 100, [repeat(toString(number % 300), 100)] FROM numbers(3000);
INSERT INTO sa_real SELECT number, number % 100, [repeat(toString(number % 300), 100)] FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_sa ON sa_est (SELECT a, b, c ORDER BY b);
SELECT '-- a stored Array(LowCardinality(String)) column';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, c FROM sa_est WHERE b < 50)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, c FROM sa_real WHERE b < 50)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

-- an insert from a table can keep the dictionary of the source and its two-byte index for the two values that the filter leaves
CREATE TABLE w_src (a UInt64, b UInt64, c LowCardinality(String)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0;
INSERT INTO w_src SELECT number, number % 100, if(number < 6000, toString(number % 2), toString(number)) FROM numbers(6300)
    SETTINGS max_block_size = 10000, max_threads = 1;
CREATE TABLE w_est AS w_src;
CREATE TABLE w_real AS w_src;
ALTER TABLE w_real ADD PROJECTION p_w (SELECT a, b, c ORDER BY b);
INSERT INTO w_est SELECT * FROM w_src WHERE c IN ('0', '1')
    SETTINGS max_threads = 1, min_insert_block_size_rows = 100000, max_insert_block_size = 100000;
INSERT INTO w_real SELECT * FROM w_src WHERE c IN ('0', '1')
    SETTINGS max_threads = 1, min_insert_block_size_rows = 100000, max_insert_block_size = 100000;
CREATE HYPOTHETICAL PROJECTION p_w ON w_est (SELECT a, b, c ORDER BY b);
SELECT '-- a LowCardinality(String) column from a filtered insert from a table';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, c FROM w_est WHERE b < 100)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT if(explain LIKE '%ReadFromMergeTree (p_w)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, c FROM w_real WHERE b < 100) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT 'real with force_optimize_projection: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, c FROM w_real WHERE b < 100 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

-- with many distinct values the dictionary is a large part of the block, and the index needs two bytes
CREATE TABLE t_est_many AS t_est;
CREATE TABLE t_real_many AS t_est;
ALTER TABLE t_real_many ADD PROJECTION p_many (SELECT a, b, v ORDER BY toLowCardinality(b));
INSERT INTO t_est_many SELECT number, number % 1000, number FROM numbers(3000);
INSERT INTO t_real_many SELECT number, number % 1000, number FROM numbers(3000);
CREATE HYPOTHETICAL PROJECTION p_many ON t_est_many (SELECT a, b, v ORDER BY toLowCardinality(b));
SELECT '-- a key with many distinct values';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est_many WHERE toLowCardinality(b) < 900)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT if(explain LIKE '%ReadFromMergeTree (p_many)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, v FROM t_real_many WHERE toLowCardinality(b) < 900) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT 'real with force_optimize_projection: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real_many WHERE toLowCardinality(b) < 900 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

-- a sample does not show the dictionary of unread rows, so a sampled LowCardinality column gets no firm verdict
CREATE TABLE m_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1000, index_granularity_bytes = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0;
CREATE TABLE m_real AS m_est;
ALTER TABLE m_real ADD PROJECTION p_unique (SELECT a, b, v ORDER BY toLowCardinality(b));
INSERT INTO m_est SELECT number, number, number FROM numbers(30000);
INSERT INTO m_real SELECT number, number, number FROM numbers(30000);
CREATE HYPOTHETICAL PROJECTION p_unique ON m_est (SELECT a, b, v ORDER BY toLowCardinality(b));
SELECT '-- a key with a distinct value in each row, with a full scan';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 0 SELECT a, b, v FROM m_est WHERE toLowCardinality(b) < 21600)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT '-- the same with a sample';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 10000 SELECT a, b, v FROM m_est WHERE toLowCardinality(b) < 21600)
WHERE match(line, '^verdict:');
SELECT if(explain LIKE '%ReadFromMergeTree (p_unique)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, v FROM m_real WHERE toLowCardinality(b) < 21600) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT 'real with force_optimize_projection: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM m_real WHERE toLowCardinality(b) < 21600 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

-- a merge gives the writer blocks with a new dictionary when it mixes the rows of its sources
-- the part has 300 values and a two-byte index, a block of the merge has at most 120 values and a one-byte index
CREATE TABLE g_est (a UInt64, b UInt64, c LowCardinality(String)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 1000, index_granularity_bytes = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 1024, use_const_adaptive_granularity = 0, enable_vertical_merge_algorithm = 0,
        merge_use_batch_sorting_queue = 0, enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE TABLE g_real AS g_est;
ALTER TABLE g_real ADD PROJECTION p_g (SELECT a, b, c ORDER BY b);
INSERT INTO g_est SELECT number, number % 100, repeat(toString(intDiv(number % 100, 20) * 100 + intHash32(number) % 60), 100) FROM numbers(4000);
INSERT INTO g_est SELECT number, number % 100, repeat(toString(intDiv(number % 100, 20) * 100 + intHash32(number) % 60), 100) FROM numbers(4000, 4000);
INSERT INTO g_real SELECT number, number % 100, repeat(toString(intDiv(number % 100, 20) * 100 + intHash32(number) % 60), 100) FROM numbers(4000);
INSERT INTO g_real SELECT number, number % 100, repeat(toString(intDiv(number % 100, 20) * 100 + intHash32(number) % 60), 100) FROM numbers(4000, 4000);
OPTIMIZE TABLE g_est FINAL;
OPTIMIZE TABLE g_real FINAL;
CREATE HYPOTHETICAL PROJECTION p_g ON g_est (SELECT a, b, c ORDER BY b);
SELECT '-- a merged part, the merge mixes the rows of its sources';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 0 SELECT a, b, c FROM g_est WHERE b < 60)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT if(explain LIKE '%ReadFromMergeTree (p_g)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, c FROM g_real WHERE b < 60) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT 'real with force_optimize_projection: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, c FROM g_real WHERE b < 60 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

-- a merge that takes whole blocks from one source keeps the dictionary of the source, and the writer counts only the index
CREATE TABLE h_est (a UInt64, b UInt64, c LowCardinality(String)) ENGINE = MergeTree ORDER BY b
    SETTINGS index_granularity = 1000, index_granularity_bytes = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 1024, use_const_adaptive_granularity = 0, enable_vertical_merge_algorithm = 0,
        merge_use_batch_sorting_queue = 0, enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE TABLE h_real AS h_est;
ALTER TABLE h_real ADD PROJECTION p_h (SELECT a, b, c ORDER BY a);
INSERT INTO h_est SELECT number, number % 100, repeat(toString(intDiv(number % 100, 20) * 100 + intHash32(number) % 60), 100) FROM numbers(4000);
INSERT INTO h_est SELECT number, number % 100, repeat(toString(intDiv(number % 100, 20) * 100 + intHash32(number) % 60), 100) FROM numbers(4000, 4000);
INSERT INTO h_real SELECT number, number % 100, repeat(toString(intDiv(number % 100, 20) * 100 + intHash32(number) % 60), 100) FROM numbers(4000);
INSERT INTO h_real SELECT number, number % 100, repeat(toString(intDiv(number % 100, 20) * 100 + intHash32(number) % 60), 100) FROM numbers(4000, 4000);
OPTIMIZE TABLE h_est FINAL;
OPTIMIZE TABLE h_real FINAL;
CREATE HYPOTHETICAL PROJECTION p_h ON h_est (SELECT a, b, c ORDER BY a);
SELECT '-- a merged part, the merge does not mix the rows of its sources';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 0 SELECT a, b, c FROM h_est WHERE a < 4800)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT if(explain LIKE '%ReadFromMergeTree (p_h)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, c FROM h_real WHERE a < 4800) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT 'real with force_optimize_projection: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, c FROM h_real WHERE a < 4800 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE t_real_lc;
DROP TABLE t_real_arr;
DROP TABLE s_est;
DROP TABLE s_real;
DROP TABLE sa_est;
DROP TABLE sa_real;
DROP TABLE w_src;
DROP TABLE w_est;
DROP TABLE w_real;
DROP TABLE t_est_many;
DROP TABLE t_real_many;
DROP TABLE m_est;
DROP TABLE m_real;
DROP TABLE g_est;
DROP TABLE g_real;
DROP TABLE h_est;
DROP TABLE h_real;
