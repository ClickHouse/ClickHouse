SELECT * FROM (SELECT col1, col2 FROM (select '1' as col1, '2' as col2) GROUP by col1, col2) AS expr_qry WHERE col2 != '';
SELECT * FROM (SELECT materialize('1') AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';
SELECT * FROM (SELECT materialize([1]) AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';
SELECT * FROM (SELECT materialize([[1]]) AS s1, materialize('2') AS s2 GROUP BY s1, s2) WHERE s2 = '2';

DROP TABLE IF EXISTS Test;

CREATE TABLE Test
ENGINE = MergeTree()
PRIMARY KEY (String1,String2)
ORDER BY (String1,String2)
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
