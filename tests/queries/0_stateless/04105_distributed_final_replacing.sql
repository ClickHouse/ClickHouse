-- Tags: no-old-analyzer
-- Regression test: distributed query plan on SELECT FINAL from engines with specialized merging
-- (Replacing, Collapsing, ...) must not reroute same-sort-key rows to different buckets, or
-- deduplication is broken.

DROP TABLE IF EXISTS t_replacing_final_correctness;

CREATE TABLE t_replacing_final_correctness(pk UInt64, version UInt64, val String) ENGINE = ReplacingMergeTree(version) ORDER BY pk;

SYSTEM STOP MERGES t_replacing_final_correctness;

-- Two parts with full PK overlap and different versions.
INSERT INTO t_replacing_final_correctness SELECT number, 1, 'old' FROM numbers(100000);
INSERT INTO t_replacing_final_correctness SELECT number, 2, 'new' FROM numbers(100000);

SELECT '-- Local';
SELECT count(), sum(version), uniqExact(pk) FROM t_replacing_final_correctness FINAL;

SELECT '-- Distributed';
SELECT count(), sum(version), uniqExact(pk) FROM t_replacing_final_correctness FINAL
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0;

DROP TABLE t_replacing_final_correctness;
