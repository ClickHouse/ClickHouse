DROP TABLE IF EXISTS t_subcolumns_join;

CREATE TABLE t_subcolumns_join (id UInt64) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO t_subcolumns_join SELECT number as number FROM numbers(10000);

SELECT
    count()
FROM (SELECT number FROM numbers(10)) as tbl LEFT JOIN t_subcolumns_join ON number = id
WHERE id is null
SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1, join_use_nulls = 1;

DROP TABLE t_subcolumns_join;
