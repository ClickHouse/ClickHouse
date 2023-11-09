CREATE TABLE IF NOT EXISTS v (values Array(Enum('foo' = 1, 'bar' = 2))) ENGINE = Memory;
INSERT INTO v VALUES (['foo', 'bar']), (['foo']), (['bar']);
SELECT * FROM v WHERE has(values, 'foo');
SELECT * FROM v WHERE hasAny(values, ['bar']);
SELECT * FROM v WHERE has(values, 'x'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
