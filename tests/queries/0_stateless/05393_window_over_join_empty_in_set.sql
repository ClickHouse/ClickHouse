-- Tags: no-parallel-replicas
-- ^ with parallel replicas the join runs on the replicas below a Union, where the filter cannot stop it.
-- Random settings limits: query_plan_merge_filters=(1, None)
-- ^ with query_plan_merge_filters = 0 the filter of the second query does not stop it.

-- A filter on a window PARTITION BY column that is always false (IN over an empty set) must stop
-- the query before the LEFT JOIN below the window reads its right side (issue #123875).
-- The right side throws if it is read.

DROP TABLE IF EXISTS t_05393_left;
DROP TABLE IF EXISTS t_05393_right;
DROP TABLE IF EXISTS t_05393_empty;
DROP TABLE IF EXISTS t_05393_keys;

CREATE TABLE t_05393_left (c1 String, c2 Date, c3 String, c4 Int64) ENGINE = MergeTree ORDER BY c2;
CREATE TABLE t_05393_right (c3 String, c5 Float64) ENGINE = MergeTree ORDER BY c3;
CREATE TABLE t_05393_empty (c1 String) ENGINE = MergeTree ORDER BY c1;
CREATE TABLE t_05393_keys (c1 String) ENGINE = MergeTree ORDER BY c1;

INSERT INTO t_05393_left VALUES ('a', '2026-01-01', 'x', 1);
INSERT INTO t_05393_right VALUES ('x', 1);
INSERT INTO t_05393_keys VALUES ('a');

-- Only a hash join that builds the right table stops early (parallel_hash, grace_hash and full_sorting_merge read it anyway).
SET join_algorithm = 'hash', query_plan_join_swap_table = 'false', parallel_hash_join_threshold = 18446744073709551615;

SELECT * FROM
(
    SELECT a.c1 AS c1, a.c2 AS c2, b.c6 AS c6, sum(a.c4) OVER (PARTITION BY a.c1, a.c2) AS c7
    FROM t_05393_left AS a
    LEFT JOIN (SELECT c3, sum(c5) AS c6 FROM t_05393_right WHERE throwIf(c5 > 0) = 0 GROUP BY c3) AS b ON b.c3 = a.c3
)
WHERE c1 IN (SELECT c1 FROM t_05393_empty)
ORDER BY c2 DESC
LIMIT 200;

SELECT count() FROM
(
    SELECT c1, c2, row_number() OVER (PARTITION BY c1 ORDER BY c2) AS rn
    FROM
    (
        SELECT a.c1 AS c1, a.c2 AS c2
        FROM t_05393_left AS a
        LEFT JOIN (SELECT c3 FROM t_05393_right WHERE throwIf(c5 > 0) = 0 GROUP BY c3) AS b ON a.c3 = b.c3
    )
)
WHERE rn = 1 AND c2 >= '2026-01-01' AND c1 IN (SELECT c1 FROM t_05393_empty);

-- With a non-empty set the right side is read.
SELECT * FROM
(
    SELECT a.c1 AS c1, a.c2 AS c2, b.c6 AS c6, sum(a.c4) OVER (PARTITION BY a.c1, a.c2) AS c7
    FROM t_05393_left AS a
    LEFT JOIN (SELECT c3, sum(c5) AS c6 FROM t_05393_right WHERE throwIf(c5 > 0) = 0 GROUP BY c3) AS b ON b.c3 = a.c3
)
WHERE c1 IN (SELECT c1 FROM t_05393_keys)
ORDER BY c2 DESC
LIMIT 200; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE t_05393_left;
DROP TABLE t_05393_right;
DROP TABLE t_05393_empty;
DROP TABLE t_05393_keys;
