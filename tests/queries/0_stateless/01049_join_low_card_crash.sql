DROP TABLE IF EXISTS Alpha;
DROP TABLE IF EXISTS Beta;

CREATE TABLE Alpha (foo String, bar UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE Beta (foo LowCardinality(String), baz UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO Alpha VALUES ('a', 1);
INSERT INTO Beta VALUES ('a', 2), ('b', 3);

SELECT * FROM Alpha FULL JOIN (SELECT 'b' as foo) js2 USING (foo) ORDER BY foo;
SELECT * FROM Alpha FULL JOIN Beta USING (foo) ORDER BY foo;
SELECT * FROM Alpha FULL JOIN Beta ON Alpha.foo = Beta.foo ORDER BY foo;

-- https://github.com/ClickHouse/ClickHouse/issues/20315#issuecomment-789579457
SELECT materialize(js2.k) FROM (SELECT toLowCardinality(number) AS k FROM numbers(1)) AS js1  FULL OUTER JOIN (SELECT number + 7 AS k FROM numbers(1)) AS js2 USING (k) ORDER BY js2.k;

SET join_use_nulls = 1;

SELECT * FROM Alpha FULL JOIN (SELECT 'b' as foo) js2 USING (foo) ORDER BY foo;
SELECT * FROM Alpha FULL JOIN Beta USING (foo) ORDER BY foo;
SELECT * FROM Alpha FULL JOIN Beta ON Alpha.foo = Beta.foo ORDER BY foo;
SELECT materialize(js2.k) FROM (SELECT toLowCardinality(number) AS k FROM numbers(1)) AS js1  FULL OUTER JOIN (SELECT number + 7 AS k FROM numbers(1)) AS js2 USING (k) ORDER BY js2.k;

DROP TABLE Alpha;
DROP TABLE Beta;
