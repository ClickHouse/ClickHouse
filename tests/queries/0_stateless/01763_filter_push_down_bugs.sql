SET allow_statistics_optimize = 0;
SELECT * FROM (SELECT col1, col2 FROM (select '1' as col1, '2' as col2) GROUP by col1, col2) AS expr_qry WHERE col2 != '';
SELECT * FROM (SELECT materialize('1') AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';
SELECT * FROM (SELECT materialize([1]) AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';
SELECT * FROM (SELECT materialize([[1]]) AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';

DROP TABLE IF EXISTS Test;

CREATE TABLE Test
ENGINE = MergeTree()
PRIMARY KEY (String1,String2)
ORDER BY (String1,String2)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'
AS
SELECT
   'String1_' || toString(number) as String1,
   'String2_' || toString(number) as String2,
   'String3_' || toString(number) as String3,
   'String4_' || toString(number%4) as String4
FROM numbers(1);

SELECT *
FROM
  (
   SELECT String1,String2,String3,String4,COUNT(*)
   FROM Test
   GROUP by String1,String2,String3,String4
  ) AS expr_qry;

SELECT *
FROM
  (
    SELECT String1,String2,String3,String4,COUNT(*)
    FROM Test
    GROUP by String1,String2,String3,String4
  ) AS expr_qry
WHERE  String4 ='String4_0';

DROP TABLE IF EXISTS Test;

select x, y from (select [0, 1, 2] as y, 1 as a, 2 as b) array join y as x where a = 1 and b = 2 and (x = 1 or x != 1) and x = 1;

DROP TABLE IF EXISTS t;
create table t(a UInt8) engine=MergeTree order by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into t select * from numbers(2);
select a from t t1 join t t2 on t1.a = t2.a where t1.a;
DROP TABLE IF EXISTS t;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (id Int64, create_time DateTime) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t2 (delete_time DateTime) ENGINE = MergeTree ORDER BY delete_time SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t1 values (101, '2023-05-28 00:00:00'), (102, '2023-05-28 00:00:00');
insert into t2 values ('2023-05-31 00:00:00');

EXPLAIN indexes=1 SELECT id, delete_time FROM t1
 CROSS JOIN (
    SELECT delete_time
    FROM t2
) AS d WHERE create_time < delete_time AND id = 101 SETTINGS enable_analyzer=0;

EXPLAIN indexes=1 SELECT id, delete_time FROM t1
 CROSS JOIN (
    SELECT delete_time
    FROM t2
) AS d WHERE create_time < delete_time AND id = 101 SETTINGS enable_analyzer=1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- expected to get row (1, 3, 1, 4) from JOIN and empty result from the query
SELECT *
FROM
(
    SELECT *
    FROM Values('id UInt64, t UInt64', (1, 3))
) AS t1
ASOF INNER JOIN
(
    SELECT *
    FROM Values('id UInt64, t UInt64', (1, 1), (1, 2), (1, 3), (1, 4), (1, 5))
) AS t2 ON (t1.id = t2.id) AND (t1.t < t2.t)
WHERE t2.t != 4;
