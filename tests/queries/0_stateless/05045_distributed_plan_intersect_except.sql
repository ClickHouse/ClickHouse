-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- INTERSECT/EXCEPT under `make_distributed_plan`: `IntersectOrExceptStep` is serializable,
-- so it runs in worker tasks instead of being rejected at the remotability gate.

DROP TABLE IF EXISTS t_ie_left;
DROP TABLE IF EXISTS t_ie_right;
CREATE TABLE t_ie_left (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_ie_right (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
-- Overlap: keys 500..999 with equal payloads. Key 1 is duplicated on both sides so that
-- ALL and DISTINCT results differ.
INSERT INTO t_ie_left SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_ie_left VALUES (1, '1');
INSERT INTO t_ie_right SELECT number, toString(number) FROM numbers(500, 1000);
INSERT INTO t_ie_right VALUES (1, '1'), (1, '1');

SET allow_experimental_analyzer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET distributed_plan_execute_locally = 1;
-- The test profile installed in CI sets a non-zero max_rows_to_group_by, which keeps
-- aggregations local. Pin it to 0 so distributed plans are exercised.
SET max_rows_to_group_by = 0;

SELECT '-- intersect distinct, rule-based';
SET enable_cascades_optimizer = 0;
SELECT count(), min(k), max(k) FROM (SELECT k, v FROM t_ie_left INTERSECT DISTINCT SELECT k, v FROM t_ie_right);

SELECT '-- intersect all, rule-based';
SELECT count() FROM (SELECT k, v FROM t_ie_left INTERSECT ALL SELECT k, v FROM t_ie_right);

SELECT '-- except distinct, rule-based';
SELECT count(), min(k), max(k) FROM (SELECT k, v FROM t_ie_left EXCEPT DISTINCT SELECT k, v FROM t_ie_right);

SELECT '-- except all, rule-based';
SELECT count() FROM (SELECT k, v FROM t_ie_left EXCEPT ALL SELECT k, v FROM t_ie_right);

SELECT '-- intersect distinct, cascades';
SET enable_cascades_optimizer = 1;
SELECT count(), min(k), max(k) FROM (SELECT k, v FROM t_ie_left INTERSECT DISTINCT SELECT k, v FROM t_ie_right);

SELECT '-- except all, cascades';
SELECT count() FROM (SELECT k, v FROM t_ie_left EXCEPT ALL SELECT k, v FROM t_ie_right);

DROP TABLE t_ie_left;
DROP TABLE t_ie_right;
