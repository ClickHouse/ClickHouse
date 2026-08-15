SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS lift_throwing_src;
DROP TABLE IF EXISTS lift_throwing_dst;
DROP TABLE IF EXISTS lift_unindexed_dst;

CREATE TABLE lift_throwing_src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE lift_throwing_dst (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE lift_unindexed_dst (k UInt64, payload String) ENGINE = MergeTree ORDER BY payload;

INSERT INTO lift_throwing_src VALUES (1);
INSERT INTO lift_throwing_dst VALUES (0), (1);
INSERT INTO lift_unindexed_dst VALUES (0, 'zero'), (1, 'one');

-- The target-only row with `k = 0` must not evaluate the throwing source predicate.
SELECT count()
FROM (SELECT * FROM lift_throwing_src WHERE intDiv(1, k) = 1) AS s
INNER JOIN lift_throwing_dst AS d ON s.k = d.k;

-- A join key outside the target primary key must not gain a full-scan lifted filter.
SELECT countIf(explain LIKE '%ilter column:%k = 1%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT count()
    FROM (SELECT * FROM lift_throwing_src WHERE k = 1) AS s
    INNER JOIN lift_unindexed_dst AS d ON s.k = d.k
);

DROP TABLE lift_throwing_src;
DROP TABLE lift_throwing_dst;
DROP TABLE lift_unindexed_dst;
