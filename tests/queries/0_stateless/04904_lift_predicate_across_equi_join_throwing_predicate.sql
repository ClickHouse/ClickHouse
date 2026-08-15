SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS lift_throwing_src;
DROP TABLE IF EXISTS lift_throwing_dst;

CREATE TABLE lift_throwing_src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE lift_throwing_dst (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO lift_throwing_src VALUES (1);
INSERT INTO lift_throwing_dst VALUES (0), (1);

-- The target-only row with `k = 0` must not evaluate the throwing source predicate.
SELECT count()
FROM (SELECT * FROM lift_throwing_src WHERE intDiv(1, k) = 1) AS s
INNER JOIN lift_throwing_dst AS d ON s.k = d.k;

DROP TABLE lift_throwing_src;
DROP TABLE lift_throwing_dst;
