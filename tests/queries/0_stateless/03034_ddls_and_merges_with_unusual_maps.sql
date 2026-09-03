-- Tests maps with "unusual" key types (Float32, Nothing, LowCardinality(String))

SET mutations_sync = 2;

DROP TABLE IF EXISTS tab;

SELECT 'Map(Nothing, ...) is non-comparable --> not usable as primary key';
CREATE TABLE tab (m1 Map(String, AggregateFunction(sum, UInt32))) ENGINE = MergeTree ORDER BY m1; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

SELECT 'But Map(Nothing, ...) can be a non-primary-key, it is quite useless though ...';
CREATE TABLE tab (m3 Map(Nothing, String)) ENGINE = MergeTree ORDER BY tuple();
-- INSERT INTO tab VALUES (map('', 'd')); -- { serverError NOT_IMPLEMENTED } -- The client can't serialize the data and fails. The query
                                                                             -- doesn't reach the server and we can't check via 'serverError' :-/
DROP TABLE tab;

SELECT 'Map(Float32, ...) and Map(LC(String)) are okay as primary key';
CREATE TABLE tab (m1 Map(Float32, String), m2 Map(LowCardinality(String), String)) ENGINE = MergeTree ORDER BY (m1, m2);
INSERT INTO tab VALUES (map(1.0, 'a'), map('b', 'b'));
INSERT INTO tab VALUES (map(2.0, 'aa'), map('bb', 'bb'));

-- Test merge
OPTIMIZE TABLE tab FINAL;
SELECT * FROM tab ORDER BY m1, m2;

DROP TABLE tab;

SELECT 'Map(Float32, ...) and Map(LC(String)) as non-primary-key';
CREATE TABLE tab (m1 Map(Float32, String), m2 Map(LowCardinality(String), String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES (map(1.0, 'a'), map('b', 'b')), (map(2.0, 'aa'), map('bb', 'bb'));
ALTER TABLE tab UPDATE m1 = map(3.0, 'aaa') WHERE m1 = map(2.0, 'aa');
SELECT * FROM tab ORDER BY m1, m2;

DROP TABLE tab;
