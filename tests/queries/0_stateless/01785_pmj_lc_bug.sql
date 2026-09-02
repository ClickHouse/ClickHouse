SET join_algorithm = 'partial_merge';
SET max_bytes_in_join = '100';

CREATE TABLE foo_lc (n LowCardinality(String)) ENGINE = Memory;
CREATE TABLE foo (n String) ENGINE = Memory;

INSERT INTO foo SELECT toString(number) AS n FROM system.numbers LIMIT 1025;
INSERT INTO foo_lc SELECT toString(number) AS n FROM system.numbers LIMIT 1025;

SELECT 1025 == count(n) FROM foo_lc AS t1 ANY LEFT JOIN foo_lc AS t2 ON t1.n == t2.n;
SELECT 1025 == count(n) FROM foo AS t1 ANY LEFT JOIN foo_lc AS t2 ON t1.n == t2.n;
SELECT 1025 == count(n) FROM foo_lc AS t1 ANY LEFT JOIN foo AS t2 ON t1.n == t2.n;

SELECT 1025 == count(n) FROM foo_lc AS t1 ALL LEFT JOIN foo_lc AS t2 ON t1.n == t2.n;

DROP TABLE foo;
DROP TABLE foo_lc;
