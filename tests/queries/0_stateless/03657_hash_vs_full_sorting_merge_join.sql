DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS tn1;
DROP TABLE IF EXISTS tn2;

CREATE TABLE t1 (key UInt32, s String) engine = TinyLog;
CREATE TABLE tn1 (key Nullable(UInt32), s String) engine = TinyLog;
CREATE TABLE t2 (key UInt32, s String) engine = TinyLog;
CREATE TABLE tn2 (key Nullable(UInt32), s String) engine = TinyLog;

INSERT INTO t1 VALUES (1, 'val1'), (2, 'val21'), (2, 'val22'), (2, 'val23'), (2, 'val24'), (2, 'val25'), (2, 'val26'), (2, 'val27'), (3, 'val3');
INSERT INTO tn1 VALUES (1, 'val1'), (NULL, 'val21'), (NULL, 'val22'), (NULL, 'val23'), (NULL, 'val24'), (NULL, 'val25'), (NULL, 'val26'), (NULL, 'val27'), (3, 'val3');
INSERT INTO t2 VALUES (1, 'val11'), (1, 'val12'), (2, 'val22'), (2, 'val23'), (2, 'val24'), (2, 'val25'), (2, 'val26'), (2, 'val27'), (2, 'val28'), (3, 'val3');
INSERT INTO tn2 VALUES (1, 'val11'), (1, 'val12'), (NULL, 'val22'), (NULL, 'val23'), (NULL, 'val24'), (NULL, 'val25'), (NULL, 'val26'), (NULL, 'val27'), (NULL, 'val28'), (3, 'val3');

SET enable_analyzer = 1;
SET join_algorithm = 'hash';

SELECT '---';
SELECT key, length(t1.s), length(t2.s) FROM t1 AS t1 ALL FULL JOIN tn2 AS t2 USING (key) ORDER BY key, length(t1.s), length(t2.s);

SET join_algorithm = 'full_sorting_merge';

SELECT '---';
SELECT key, length(t1.s), length(t2.s) FROM t1 AS t1 ALL FULL JOIN tn2 AS t2 USING (key) ORDER BY key, length(t1.s), length(t2.s);

SET join_use_nulls;

SET join_algorithm = 'hash';

SELECT '---';
SELECT key, length(t1.s), length(t2.s) FROM t1 AS t1 ALL FULL JOIN tn2 AS t2 USING (key) ORDER BY key, length(t1.s), length(t2.s);

SET join_algorithm = 'full_sorting_merge';

SELECT '---';
SELECT key, length(t1.s), length(t2.s) FROM t1 AS t1 ALL FULL JOIN tn2 AS t2 USING (key) ORDER BY key, length(t1.s), length(t2.s);


DROP TABLE t1;
DROP TABLE t2;
DROP TABLE tn1;
DROP TABLE tn2;
