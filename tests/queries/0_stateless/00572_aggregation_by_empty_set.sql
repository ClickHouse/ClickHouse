CREATE TEMPORARY TABLE t (x UInt8);

SET empty_result_for_aggregation_by_empty_set = 0;

SELECT count() FROM system.one WHERE 0;
SELECT count() FROM system.one WHERE rand() < 0;
SELECT count() FROM system.one WHERE 1;

SELECT count(), uniq(x), avg(x), avg(toNullable(x)), groupArray(x), groupUniqArray(x) FROM t;
SELECT count(), uniq(x), avg(x), avg(toNullable(x)), groupArray(x), groupUniqArray(x) FROM (SELECT * FROM t UNION ALL SELECT * FROM t);
SELECT x, count(), uniq(x), avg(x), avg(toNullable(x)), groupArray(x), groupUniqArray(x) FROM t GROUP BY x;

SET empty_result_for_aggregation_by_empty_set = 1;

SELECT count() FROM system.one WHERE 0;
SELECT count() FROM system.one WHERE rand() < 0;
SELECT count() FROM system.one WHERE 1;

SELECT count(), uniq(x), avg(x), avg(toNullable(x)), groupArray(x), groupUniqArray(x) FROM t;
SELECT count(), uniq(x), avg(x), avg(toNullable(x)), groupArray(x), groupUniqArray(x) FROM (SELECT * FROM t UNION ALL SELECT * FROM t);
SELECT x, count(), uniq(x), avg(x), avg(toNullable(x)), groupArray(x), groupUniqArray(x) FROM t GROUP BY x;
