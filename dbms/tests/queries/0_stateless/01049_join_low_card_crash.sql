DROP TABLE IF EXISTS Alpha;
DROP TABLE IF EXISTS Beta;

CREATE TABLE Alpha (foo String, bar UInt64) ENGINE = Memory;
CREATE TABLE Beta (foo LowCardinality(String), baz UInt64) ENGINE = Memory;

INSERT INTO Alpha VALUES ('a', 1);
INSERT INTO Beta VALUES ('a', 2), ('b', 3);

SELECT * FROM Alpha FULL JOIN (SELECT 'b' as foo) js2 USING (foo) ORDER BY foo;
SELECT * FROM Alpha FULL JOIN Beta USING (foo) ORDER BY foo;
SELECT * FROM Alpha FULL JOIN Beta ON Alpha.foo = Beta.foo ORDER BY foo;

SET join_use_nulls = 1;

SELECT * FROM Alpha FULL JOIN (SELECT 'b' as foo) js2 USING (foo) ORDER BY foo;
SELECT * FROM Alpha FULL JOIN Beta USING (foo) ORDER BY foo;
SELECT * FROM Alpha FULL JOIN Beta ON Alpha.foo = Beta.foo ORDER BY foo;

DROP TABLE Alpha;
DROP TABLE Beta;
