-- Tags: no-parallel

SET allow_experimental_analyzer = 0, allow_experimental_part_aggregation_cache = 1;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache;
CREATE TABLE t_part_agg_cache (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_part_agg_cache VALUES (1, 10), (2, 20), (1, 30);
INSERT INTO t_part_agg_cache VALUES (1, 40), (2, 50);

SELECT count() FROM system.part_aggregation_cache;

SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k
;

SELECT count() > 0 FROM system.part_aggregation_cache;

SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k
;

INSERT INTO t_part_agg_cache VALUES (1, 100), (2, 200);

SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k
;

SYSTEM DROP PART AGGREGATION CACHE;
SELECT count() FROM system.part_aggregation_cache;

-- Test with WHERE: k=1: 30+40+100=170, k=2: 20+50+200=270
SELECT k, sum(v) FROM t_part_agg_cache WHERE v > 10 GROUP BY k ORDER BY k
;

DROP TABLE t_part_agg_cache;
SYSTEM DROP PART AGGREGATION CACHE;
