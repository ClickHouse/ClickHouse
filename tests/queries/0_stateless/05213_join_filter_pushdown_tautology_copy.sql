-- A `WHERE` equality over two keys that the join equates to the same key of the other side is copied to
-- that side as `k = k`. The copy is dropped: it is implied by the join, and the statistics would price it as
-- a real predicate. The conjunct itself stays on the side where it is not substituted.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (1), (2), (NULL);
INSERT INTO t2 VALUES (1), (3), (NULL);
INSERT INTO t3 VALUES (1), (2), (3), (NULL);

SET enable_analyzer = 1;
SET query_plan_merge_filter_into_join_condition = 0;

SELECT '-- the copy is not pushed to the other side';
SELECT countIf(explain LIKE '%equals(__table3.k, __table3.k)%')
FROM (EXPLAIN actions = 1 SELECT * FROM t1 JOIN t3 ON t1.k = t3.k JOIN t2 ON t2.k = t3.k WHERE t1.k = t2.k);

SELECT '-- results do not depend on the pushdown';
SELECT * FROM t1 JOIN t3 ON t1.k = t3.k JOIN t2 ON t2.k = t3.k WHERE t1.k = t2.k ORDER BY ALL;
SELECT * FROM t1 JOIN t3 ON t1.k = t3.k JOIN t2 ON t2.k = t3.k WHERE t1.k = t2.k ORDER BY ALL SETTINGS query_plan_filter_push_down = 0;

SELECT '-- null-safe keys: the NULL match is rejected by `WHERE`, with and without the pushdown';
SELECT * FROM t1 JOIN t3 ON t1.k <=> t3.k JOIN t2 ON t2.k <=> t3.k WHERE t1.k = t2.k ORDER BY ALL;
SELECT * FROM t1 JOIN t3 ON t1.k <=> t3.k JOIN t2 ON t2.k <=> t3.k WHERE t1.k = t2.k ORDER BY ALL SETTINGS query_plan_filter_push_down = 0;

SELECT '-- `WHERE` restating a null-safe join key keeps rejecting the NULL match';
SELECT * FROM t1 JOIN t2 ON t1.k <=> t2.k WHERE t1.k = t2.k ORDER BY ALL;
SELECT * FROM t1 JOIN t2 ON t1.k <=> t2.k WHERE t1.k = t2.k ORDER BY ALL SETTINGS query_plan_filter_push_down = 0;
SELECT * FROM t1 JOIN t2 ON t1.k <=> t2.k ORDER BY ALL;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
