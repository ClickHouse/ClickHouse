-- Tags: replica, no-replicated-database

-- Test: dedup token with INSERT SELECT (no ORDER BY ALL) + materialized views
-- This covers the interaction between insert_deduplication_token and
-- deduplicate_blocks_in_dependent_materialized_views for unsorted INSERT SELECT


DROP TABLE IF EXISTS t_src_04070 SYNC;
DROP TABLE IF EXISTS t_mv_dst_04070 SYNC;
DROP VIEW IF EXISTS mv_04070;

CREATE TABLE t_src_04070 (k UInt64, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_src_04070_99206/r1', 'r1')
ORDER BY k;

CREATE TABLE t_mv_dst_04070 (k UInt64, cnt UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_mv_dst_04070_99206/r1', 'r1')
ORDER BY k;

CREATE MATERIALIZED VIEW mv_04070 TO t_mv_dst_04070 AS
SELECT k, count() as cnt FROM t_src_04070 GROUP BY k;

-- Insert with dedup token, no ORDER BY ALL, with MV dedup enabled
SELECT 'MV dedup with token, no ORDER BY ALL';
INSERT INTO t_src_04070 SELECT number, 'a' FROM numbers(10) SETTINGS insert_deduplication_token = 'mv_token1', deduplicate_blocks_in_dependent_materialized_views = 1;
INSERT INTO t_src_04070 SELECT number, 'b' FROM numbers(10) SETTINGS insert_deduplication_token = 'mv_token1', deduplicate_blocks_in_dependent_materialized_views = 1;
SELECT count() FROM t_src_04070 ORDER BY 1;
SELECT count() FROM t_mv_dst_04070 ORDER BY 1;

-- Different token inserts new data into both source and MV
SELECT 'Different token inserts into MV';
INSERT INTO t_src_04070 SELECT number, 'c' FROM numbers(10) SETTINGS insert_deduplication_token = 'mv_token2', deduplicate_blocks_in_dependent_materialized_views = 1;
SELECT count() FROM t_src_04070 ORDER BY 1;
SELECT count() FROM t_mv_dst_04070 ORDER BY 1;

DROP VIEW mv_04070;
DROP TABLE t_mv_dst_04070 SYNC;
DROP TABLE t_src_04070 SYNC;
