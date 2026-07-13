DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS v2;

CREATE TABLE IF NOT EXISTS v (value Array(Enum('foo' = 1, 'bar' = 2))) ENGINE = Memory;
INSERT INTO v VALUES (['foo', 'bar']), (['foo']), (['bar']);
SELECT * FROM v WHERE has(value, 'foo') ORDER BY value;
SELECT * FROM v WHERE hasAny(value, ['bar']) ORDER BY value;
SELECT * FROM v WHERE has(value, 'x') ORDER BY value;

CREATE TABLE IF NOT EXISTS v2 (value Array(Array(Nullable(Enum('foo' = 1, 'bar' = 2))))) ENGINE = Memory;
INSERT INTO v2 VALUES ([['foo', 'bar']]), ([['foo']]), ([['bar']]);
SELECT * FROM v2 WHERE has(value, ['foo']) ORDER BY value;
SELECT * FROM v2 WHERE has(value, [NULL]) ORDER BY value;

DROP TABLE v;
DROP TABLE v2;
