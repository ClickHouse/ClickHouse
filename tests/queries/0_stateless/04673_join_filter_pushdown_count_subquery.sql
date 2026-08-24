-- Left-only WHERE on `count()` of `SELECT * … JOIN` must still be pushed through
-- the JOIN (and composed through identifier-rename expressions) so the left
-- read can apply PREWHERE / index analysis.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left
(
    a Int32,
    b Int32
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi';

CREATE TABLE t_right
(
    a Int32,
    b Int32
)
ENGINE = Memory;

INSERT INTO t_left SELECT number, number FROM numbers(100);
INSERT INTO t_right SELECT number, number FROM numbers(100);

SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET enable_join_runtime_filters = 0;
SET join_use_nulls = 1;

SELECT count()
FROM
(
    SELECT *
    FROM t_left AS foo
    LEFT JOIN t_right AS bar ON foo.b = bar.b
    WHERE foo.a < 40
);

SELECT throwIf(count() = 0)
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM
    (
        SELECT *
        FROM t_left AS foo
        LEFT JOIN t_right AS bar ON foo.b = bar.b
        WHERE foo.a < 40
    )
)
WHERE explain ILIKE '%Prewhere%'
FORMAT Null;

SELECT count()
FROM
(
    SELECT *
    FROM (SELECT * FROM t_left) AS foo
    LEFT JOIN t_right AS bar ON foo.b = bar.b
    WHERE foo.a < 40
);

SELECT throwIf(count() = 0)
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM
    (
        SELECT *
        FROM (SELECT * FROM t_left) AS foo
        LEFT JOIN t_right AS bar ON foo.b = bar.b
        WHERE foo.a < 40
    )
)
WHERE explain ILIKE '%Prewhere%'
FORMAT Null;

DROP TABLE t_left;
DROP TABLE t_right;
