DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (`id` Int32, key String, key2 String) ENGINE = TinyLog;
CREATE TABLE t2 (`id` Int32, key String, key2 String) ENGINE = TinyLog;

INSERT INTO t1 VALUES (1, '111', '111'),(2, '222', '2'),(2, '222', '222'),(3, '333', '333');
INSERT INTO t2 VALUES (2, 'AAA', 'AAA'),(2, 'AAA', 'a'),(3, 'BBB', 'BBB'),(4, 'CCC', 'CCC');

SELECT '-- hash_join --';

SELECT '--';
SELECT t1.key, t1.key2 FROM t1 INNER ALL JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2;
SELECT '--';
SELECT t1.key, t1.key2 FROM t1 INNER ALL JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2;

SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2;
SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 AND t2.id > 2;
SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 AND t2.id == 3;
SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 AND t2.key2 == 'BBB';
SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 AND t1.key2 == '333';

SELECT '--';
SELECT t1.id, t1.key, t1.key2, t2.id, t2.key, t2.key2  FROM t1 FULL JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 ORDER BY t1.id, t2.id;

SET join_algorithm = 'partial_merge';

SELECT '-- partial_merge --';

SELECT '--';
SELECT t1.key, t1.key2 FROM t1 INNER ALL JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2;
SELECT '--';
SELECT t1.key, t1.key2 FROM t1 INNER ALL JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2;

SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2;
SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 AND t2.id > 2;
SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 AND t2.id == 3;
SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 AND t2.key2 == 'BBB';
SELECT '--';
SELECT t1.key FROM t1 INNER ANY JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 AND t1.key2 == '333';

SELECT '--';
SELECT t1.id, t1.key, t1.key2, t2.id, t2.key, t2.key2  FROM t1 FULL JOIN t2 ON t1.id == t2.id AND t2.key == t2.key2 AND t1.key == t1.key2 ORDER BY t1.id, t2.id;
