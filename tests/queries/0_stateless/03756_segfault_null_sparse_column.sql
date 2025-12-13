-- There were crashes in AggregateFunctionNullUnary when column type is Nullable
-- but actual column is not wrapped in ColumnNullable (e.g. sparse column values)

SET enable_analyzer=1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (
    a String,
    b String DEFAULT ''
) ENGINE = MergeTree ORDER BY b
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

CREATE TABLE t2 (b String) ENGINE = MergeTree ORDER BY b;

CREATE TABLE t3 (id String, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 (a, b) SELECT toString(number), '' FROM numbers(5000);
INSERT INTO t1 (a, b) SELECT toString(number+5000), concat('k', toString(number % 100)) FROM numbers(500);

INSERT INTO t2 SELECT concat('k', toString(number % 50)) FROM numbers(200);

INSERT INTO t3 VALUES ('t1', 'c1'), ('t2', 'c2'), ('t3', '');

OPTIMIZE TABLE t1 FINAL;
OPTIMIZE TABLE t2 FINAL;

WITH cte AS (
    SELECT id, max(CASE WHEN value != '' THEN value END) as b
    FROM t3
    GROUP BY id
)
SELECT a, count(distinct b), count(distinct t2.b), count(distinct cte.b)
FROM t1
LEFT JOIN t2 USING(b)
LEFT JOIN cte USING(b)
GROUP BY a
ORDER BY a
LIMIT 5
SETTINGS max_threads=1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
