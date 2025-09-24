DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id UInt64, s1 Nullable(String), s2 Nullable(String)) ORDER BY id;
CREATE TABLE t2 (id UInt64, s1 String, s2 String) ORDER BY id;

INSERT INTO t1 VALUES (1, 'a', 'b'), (2, 'c', 'd'), (3, 'e', null);
INSERT INTO t2 VALUES (4, 'z', 'y'), (5, 'x', 'w');

SELECT id FROM t1 WHERE (s1, s2) = ('a', 'b');
SELECT id FROM t1 WHERE (s1, s2) = '(\'a\',\'b\')';
SELECT id FROM t1 WHERE (s1, s2) = CAST((SELECT s1, s2 FROM t1 WHERE s1 = 'a') AS text);
SELECT id FROM t1 WHERE (s1, null) = ('a', null);
SELECT id FROM t1 WHERE (s1, null) = '(\'a\',null)';
SELECT id FROM t1 WHERE (s1, null) = CAST((SELECT s1, s2 FROM t1 WHERE s1 = 'e' and s2 is null) AS text);

SELECT id FROM t2 WHERE (s1, null) = ('z', null);
SELECT id FROM t2 WHERE (s1, null) = '(\'z\',null)';

DROP TABLE t1;
DROP TABLE t2;
