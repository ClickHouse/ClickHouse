DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int32, c1 Int32, c2 String) ENGINE = Log() ;
INSERT INTO t0(c0, c1, c2) VALUES (826636805,0, ''), (0, 150808457, '');

SELECT 'DATA';
SELECT * FROM t0 FORMAT PrettyMonoBlock;

SELECT 'NUMBER OF ROWS IN FIRST SHOULD BE EQUAL TO SECOND';


SELECT 'FISRT';
SELECT left.c2 FROM t0 AS left
LEFT ANTI JOIN t0 AS right_0 ON ((left.c0)=(right_0.c1))
WHERE (abs ((- ((sign (right_0.c1))))));

SELECT 'SECOND';
SELECT SUM(check <> 0)
FROM
(
  SELECT (abs ((- ((sign (right_0.c1)))))) AS `check`
  FROM t0 AS left
  LEFT ANTI JOIN t0 AS right_0 ON ((left.c0)=(right_0.c1))
);


SELECT 'TO DEBUG I TOOK JUST A SUBQUERY AND IT HAS 1 ROW';

SELECT 'THIRD';

SELECT (abs ((- ((sign (right_0.c1)))))) AS `check`
FROM t0 AS left
LEFT ANTI JOIN t0 AS right_0 ON ((left.c0)=(right_0.c1));


SELECT 'AND I ADDED SINGLE CONDITION THAT CONDITION <>0 THAT IS 1 IN THIRD QUERY AND IT HAS NO RESULT!!!';


SELECT 'FOURTH';
SELECT (abs ((- ((sign (right_0.c1)))))) AS `check`
FROM t0 AS left
LEFT ANTI JOIN t0 AS right_0 ON ((left.c0)=(right_0.c1))
WHERE check <> 0;

DROP TABLE t0;
