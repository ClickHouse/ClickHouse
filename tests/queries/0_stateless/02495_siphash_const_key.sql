DROP TABLE IF EXISTS sipHashKeyed_test;

CREATE TABLE sipHashKeyed_test
ENGINE = Memory()
AS
SELECT
1 a,
'test' b;

SELECT
sipHash64Keyed((toUInt64(0), toUInt64(0)), 1, 'test');

SELECT
sipHash64(tuple(*))
FROM
sipHashKeyed_test;

SELECT
sipHash64Keyed((toUInt64(0), toUInt64(0)), tuple(*))
FROM
sipHashKeyed_test;

SELECT
sipHash64Keyed((toUInt64(0), toUInt64(0)), a, b)
FROM
sipHashKeyed_test;

SELECT
hex(sipHash128Keyed((toUInt64(0), toUInt64(0)), tuple(*)))
FROM
sipHashKeyed_test;

SELECT
hex(sipHash128Keyed((toUInt64(0), toUInt64(0)), a, b))
FROM
sipHashKeyed_test;
