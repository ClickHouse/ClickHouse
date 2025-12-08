-- There were crashes in AggregateFunctionNullUnary when column type is Nullable
-- but actual column is not wrapped in ColumnNullable (e.g. sparse column values)

DROP TABLE IF EXISTS t_sparse_main;
DROP TABLE IF EXISTS t_sparse_lookup;
DROP TABLE IF EXISTS t_sparse_meta;

CREATE TABLE t_sparse_main (
    key1 String,
    key2 String DEFAULT ''
) ENGINE = MergeTree ORDER BY key2;

CREATE TABLE t_sparse_lookup (
    key2 String
) ENGINE = MergeTree ORDER BY key2;

CREATE TABLE t_sparse_meta (
    id String,
    value String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_sparse_main (key1, key2) SELECT toString(number), '' FROM numbers(5000);

INSERT INTO t_sparse_lookup SELECT concat('k', toString(number % 50)) FROM numbers(200);

INSERT INTO t_sparse_meta VALUES ('a', '');

-- count(distinct) on columns from LEFT JOIN
-- The CTE produces Nullable(String) from CASE expression
-- When joined, this causes AggregateFunctionNullUnary to be used
-- but with sparse columns that have non-nullable values
WITH
cte AS (
    SELECT
        id,
        max(CASE WHEN value != '' THEN value END) as key2
    FROM t_sparse_meta
    GROUP BY id
)
SELECT
    key1,
    count(distinct key2) as cnt1,
    count(distinct t_sparse_lookup.key2) as cnt2,
    count(distinct cte.key2) as cnt3
FROM t_sparse_main
LEFT JOIN t_sparse_lookup USING(key2)
LEFT JOIN cte USING(key2)
GROUP BY key1
ORDER BY key1
LIMIT 5
SETTINGS max_threads=1;

DROP TABLE t_sparse_main;
DROP TABLE t_sparse_lookup;
DROP TABLE t_sparse_meta;
