-- Coverage test for src/Analyzer/TableExpressionModifiers.cpp:
--   lines 27-28   dump() with sample_offset_ratio (EXPLAIN QUERY TREE path)
-- Existing tests only use SAMPLE without OFFSET; the sample_offset_ratio branch in dump() is never hit.
-- Tags: no-parallel-replicas

SET enable_analyzer = 1; -- targeted code runs only in the analyzer path; pin it so old-analyzer CI shards behave the same
CREATE TABLE t_sample_off (a UInt64) ENGINE = MergeTree ORDER BY a SAMPLE BY a;
INSERT INTO t_sample_off SELECT number FROM numbers(100);

-- EXPLAIN QUERY TREE calls TableExpressionModifiers::dump() — hits lines 27-28
EXPLAIN QUERY TREE SELECT * FROM t_sample_off SAMPLE 1/2 OFFSET 1/4;

-- EXPLAIN SYNTAX verifies the SAMPLE ... OFFSET syntax round-trips correctly via the AST formatter
EXPLAIN SYNTAX SELECT * FROM t_sample_off SAMPLE 1/2 OFFSET 1/4;

DROP TABLE t_sample_off;
