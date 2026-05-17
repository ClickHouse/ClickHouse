SET enable_analyzer = 1;
DROP TABLE IF EXISTS t_count_distinct_tuple;

CREATE TABLE t_count_distinct_tuple (tup Tuple(s String, u UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_count_distinct_tuple SELECT (toString(number), number) FROM numbers(100);

SELECT countDistinct(tup.s) FROM t_count_distinct_tuple SETTINGS count_distinct_optimization = 1;
SELECT countDistinct(tup.u) FROM t_count_distinct_tuple SETTINGS count_distinct_optimization = 1;
SELECT DISTINCT countDistinct(tup.s) FROM t_count_distinct_tuple SETTINGS count_distinct_optimization = 1;

-- Exercise the QueryNode branch in PlannerJoinTree (subquery with only a tuple subcolumn projection).
SELECT countDistinct(tup.s) FROM (SELECT tup FROM t_count_distinct_tuple) SETTINGS count_distinct_optimization = 1;

-- Exercise the UnionNode branch in PlannerJoinTree (UNION ALL of subqueries with only a tuple subcolumn projection).
SELECT countDistinct(tup.s) FROM (
    SELECT tup FROM t_count_distinct_tuple
    UNION ALL
    SELECT tup FROM t_count_distinct_tuple
) SETTINGS count_distinct_optimization = 1;

DROP TABLE t_count_distinct_tuple;
