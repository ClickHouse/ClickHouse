DROP TABLE IF EXISTS t;
create table t( s String ) Engine=Memory as select arrayJoin (['a','b','c']);

SELECT round((sum(multiIf(s IN ('a', 'b'), 1, 0)) / count()) * 100) AS r
FROM cluster('test_cluster_two_shards', currentDatabase(), t);

DROP TABLE t;


DROP TABLE IF EXISTS test_alias;

CREATE TABLE test_alias(`a` Int64, `b` Int64, `c` Int64, `day` Date, `rtime` DateTime) ENGINE = Memory
as select 0, 0, 0, '2022-01-01', 0 from zeros(10);

WITH 
    sum(if((a >= 0) AND (b != 100) AND (c = 0), 1, 0)) AS r1, 
    sum(if((a >= 0) AND (b != 100) AND (c > 220), 1, 0)) AS r2 
SELECT 
    (intDiv(toUInt32(rtime), 20) * 20) * 1000 AS t, 
    (r1 * 100) / (r1 + r2) AS m
FROM cluster('test_cluster_two_shards', currentDatabase(), test_alias)
WHERE day = '2022-01-01'
GROUP BY t
ORDER BY t ASC;

DROP TABLE test_alias;
