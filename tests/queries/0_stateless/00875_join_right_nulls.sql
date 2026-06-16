DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS nt;

CREATE TABLE t (x String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE nt (x Nullable(String)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t (x) VALUES ('id'), ('1');
INSERT INTO nt (x) VALUES ('id'), (NULL), ('1');


SET join_use_nulls = 1;

SELECT 'on';

SELECT 'n rj n', t1.x, t2.x FROM nt AS t1 RIGHT JOIN nt AS t2 ON t1.x = t2.x ORDER BY t1.x;
SELECT 'n fj n', t1.x, t2.x FROM nt AS t1 FULL JOIN nt AS t2 ON t1.x = t2.x ORDER BY t1.x;

SELECT 't rj n', t1.x, t2.x FROM t AS t1 RIGHT JOIN nt AS t2 ON t1.x = t2.x ORDER BY t1.x;
SELECT 't fj n', t1.x, t2.x FROM t AS t1 FULL JOIN nt AS t2 ON t1.x = t2.x ORDER BY t1.x;

SELECT 'n rj t', t1.x, t2.x FROM nt AS t1 RIGHT JOIN t AS t2 ON t1.x = t2.x ORDER BY t1.x;
SELECT 'n fj t', t1.x, t2.x FROM nt AS t1 FULL JOIN t AS t2 ON t1.x = t2.x ORDER BY t1.x;

SELECT 'using';

SELECT 'n rj n', t1.x, t2.x FROM nt AS t1 RIGHT JOIN nt AS t2 USING(x) ORDER BY t1.x;
SELECT 'n fj n', t1.x, t2.x FROM nt AS t1 FULL JOIN nt AS t2 USING(x) ORDER BY t1.x;

SELECT 't rj n', t1.x, t2.x FROM t AS t1 RIGHT JOIN nt AS t2 USING(x) ORDER BY t1.x;
SELECT 't fj n', t1.x, t2.x FROM t AS t1 FULL JOIN nt AS t2 USING(x) ORDER BY t1.x;

SELECT 'n rj t', t1.x, t2.x FROM nt AS t1 RIGHT JOIN t AS t2 USING(x) ORDER BY t1.x;
SELECT 'n fj t', t1.x, t2.x FROM nt AS t1 FULL JOIN t AS t2 USING(x) ORDER BY t1.x;


INSERT INTO nt (x) SELECT NULL as x FROM numbers(1000);

SELECT sum(isNull(t1.x)), count(t1.x) FROM nt AS t1 INNER JOIN nt AS t2 ON t1.x = t2.x;
SELECT sum(isNull(t1.x)), count(t1.x) FROM nt AS t1 LEFT JOIN nt AS t2 ON t1.x = t2.x;
SELECT sum(isNull(t1.x)), count(t1.x) FROM nt AS t1 RIGHT JOIN nt AS t2 ON t1.x = t2.x;
SELECT sum(isNull(t1.x)), count(t1.x) FROM nt AS t1 FULL JOIN nt AS t2 ON t1.x = t2.x;

SELECT sum(isNull(t1.x)), count(t1.x) FROM nt AS t1 INNER JOIN nt AS t2 USING(x);
SELECT sum(isNull(t1.x)), count(t1.x) FROM nt AS t1 LEFT JOIN nt AS t2 USING(x);
SELECT sum(isNull(t1.x)), count(t1.x) FROM nt AS t1 RIGHT JOIN nt AS t2 USING(x);
SELECT sum(isNull(t1.x)), count(t1.x) FROM nt AS t1 FULL JOIN nt AS t2 USING(x);

DROP TABLE t;
DROP TABLE nt;
