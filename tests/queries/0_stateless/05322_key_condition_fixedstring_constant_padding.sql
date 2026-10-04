-- Casting a `FixedString` constant or set element to `String` drops its trailing zero bytes, so it must not be
-- pushed through a key expression this way: `s <= toFixedString('abc', 4)` would become `key <= 'abc'`. The same
-- holds for a `FixedString` nested in another type or held by a `Variant` or `Dynamic` value.

DROP TABLE IF EXISTS t_monotonic;
DROP TABLE IF EXISTS t_deterministic;
DROP TABLE IF EXISTS t_bare;
DROP TABLE IF EXISTS t_array;

CREATE TABLE t_monotonic (s Nullable(String))
ENGINE = MergeTree ORDER BY assumeNotNull(s) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_monotonic VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

CREATE TABLE t_deterministic (s String)
ENGINE = MergeTree ORDER BY cityHash64(s) SETTINGS index_granularity = 1;
INSERT INTO t_deterministic VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

CREATE TABLE t_bare (s String)
ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO t_bare VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

CREATE TABLE t_array (s Array(String))
ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO t_array VALUES (['abc']), (['abc\0']), (['abc\0']), (['b']);

-- { echo }

SELECT count() FROM t_monotonic WHERE s <= toFixedString('abc', 4);
SELECT count() FROM t_deterministic WHERE s = toFixedString('abc', 4);
SELECT count() FROM t_deterministic WHERE s IN (SELECT toFixedString('abc', 4));
SELECT count() FROM t_deterministic WHERE s = CAST(toFixedString('abc', 4) AS Variant(FixedString(4), UInt64));
SELECT count() FROM t_deterministic WHERE s = CAST(toFixedString('abc', 4) AS Dynamic);
SELECT count() FROM t_bare WHERE s IN (SELECT toFixedString('abc', 4));
SELECT count() FROM t_bare WHERE s IN (SELECT toNullable(toFixedString('abc', 4)));
SELECT count() FROM t_array WHERE s IN (SELECT [toFixedString('abc', 4)]);

DROP TABLE t_monotonic;
DROP TABLE t_deterministic;
DROP TABLE t_bare;
DROP TABLE t_array;
