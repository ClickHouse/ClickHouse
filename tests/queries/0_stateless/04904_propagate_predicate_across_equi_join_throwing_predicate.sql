SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS prop_throwing_src;
DROP TABLE IF EXISTS prop_throwing_dst;
DROP TABLE IF EXISTS prop_unindexed_dst;

CREATE TABLE prop_throwing_src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prop_throwing_dst (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prop_unindexed_dst (k UInt64, payload String) ENGINE = MergeTree ORDER BY payload;

INSERT INTO prop_throwing_src VALUES (1);
INSERT INTO prop_throwing_dst VALUES (0), (1);
INSERT INTO prop_unindexed_dst VALUES (0, 'zero'), (1, 'one');

-- The target-only row `k = 0` must not evaluate the throwing predicate
SELECT count()
FROM (SELECT * FROM prop_throwing_src WHERE intDiv(1, k) = 1) AS s
INNER JOIN prop_throwing_dst AS d ON s.k = d.k;

-- A join key outside the target primary key must not gain a full scan filter
SELECT countIf(explain LIKE '%ilter column:%k = 1%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT count()
    FROM (SELECT * FROM prop_throwing_src WHERE k = 1) AS s
    INNER JOIN prop_unindexed_dst AS d ON s.k = d.k
);

DROP TABLE prop_throwing_src;
DROP TABLE prop_throwing_dst;
DROP TABLE prop_unindexed_dst;
