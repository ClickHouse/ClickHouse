-- The join-order conflict detectors reorder outer joins using null-rejection (paper Definition 1),
-- which is only sound when an unmatched outer-join row is padded with a real SQL NULL. Under the
-- default join_use_nulls = 0 the padding is a type default (0/''), so a "null-rejecting" predicate
-- like t2.id = t3.id still matches the padded 0, and reordering (t1 LEFT JOIN t2) LEFT JOIN t3 into
-- t1 LEFT JOIN (t2 LEFT JOIN t3) changes the result. Regression test for
-- https://github.com/ClickHouse/ClickHouse/issues/118520: the detector must return exactly the
-- unoptimized rows. The counts and the padded-key row below are the unoptimized (limit = 0) results;
-- with the detector on they must be unchanged.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (0,'v1_0'),(1,'v1_1'),(2,'v1_2');
INSERT INTO t2 VALUES (0,'v2_0'),(1,'v2_1'),(3,'v2_3');
INSERT INTO t3 VALUES (0,'v3_0'),(1,'v3_1'),(4,'v3_4');

SET enable_analyzer = 1, single_join_prefer_left_table = 0;
-- Make t1 look expensive so the detector prefers to reorder t2/t3 to the front.
SET param__internal_join_table_stat_hints = '{"t1": {"cardinality": 100000, "distinct_keys": {"id": 2}}, "t2": {"cardinality": 3, "distinct_keys": {"id": 3}}, "t3": {"cardinality": 3, "distinct_keys": {"id": 3}}}';

SELECT 'LEFT+LEFT   cd_c', count() FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;
SELECT 'LEFT+LEFT   cd_a', count() FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1;

-- The row that exposed the bug: t1.id = 2 has no t2 match, so t2.id is padded 0; the downstream
-- t2.id = t3.id then legitimately matches t3.id = 0 under join_use_nulls = 0. The reorder must keep
-- t3.value = 'v3_0' here, not drop it to ''.
SELECT 'padded-key row  ', t1.id, t2.id, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id
WHERE t1.id = 2
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

-- Cross-boundary inner predicate: an inner join whose ON clause references a relation on the far
-- side of an outer-join boundary (t1.z = t3.z, where t1 is the outer join's null side). With a
-- detector on, DPsub used to report "Failed to find a valid join order" (Code 717) for these shapes
-- because the inner operator's ON clause spans {t1,t2,t3} and was wrongly applied at the {t1}|{t2}
-- split before t3 was joined. The detector must now plan them and return exactly the unoptimized
-- rows. These queries run the CD path (they enable a conflict detector), unlike 04305 which uses
-- plain dpsub, so they guard the fix in CI.
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 (x UInt64, z UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t2 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t3 (y UInt64, z UInt64) ENGINE = MergeTree ORDER BY y;
INSERT INTO t1 VALUES (1, 100), (2, 300), (3, 500);
INSERT INTO t2 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t3 VALUES (10, 999), (20, 300), (30, 500);
-- Make t2 (the null-supplying relation) look huge so the detector is tempted to join t1/t3 first.
SET param__internal_join_table_stat_hints = '{"t1": {"cardinality": 3}, "t2": {"cardinality": 1000000}, "t3": {"cardinality": 3}}';

SELECT 'LEFT  cross-boundary cd_c', t1.x, t1.z, t2.x, t2.y, t3.y, t3.z
FROM t1 LEFT JOIN t2 ON t1.x = t2.x JOIN t3 ON t2.y = t3.y AND t1.z = t3.z ORDER BY ALL
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;
SELECT 'RIGHT cross-boundary cd_a', t1.x, t1.z, t2.x, t2.y, t3.y, t3.z
FROM t1 RIGHT JOIN t2 ON t1.x = t2.x JOIN t3 ON t2.y = t3.y AND t1.z = t3.z ORDER BY ALL
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
