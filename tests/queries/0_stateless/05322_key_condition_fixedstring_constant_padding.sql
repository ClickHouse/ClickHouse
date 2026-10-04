-- Casting a `FixedString` constant or set element to `String` drops its trailing zero bytes, so it must not be
-- pushed through a key expression this way: `s <= toFixedString('abc', 4)` would become `key <= 'abc'`.

DROP TABLE IF EXISTS t_monotonic;
DROP TABLE IF EXISTS t_deterministic;
DROP TABLE IF EXISTS t_bare;

CREATE TABLE t_monotonic (s Nullable(String))
ENGINE = MergeTree ORDER BY assumeNotNull(s) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_monotonic VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

CREATE TABLE t_deterministic (s String)
ENGINE = MergeTree ORDER BY cityHash64(s) SETTINGS index_granularity = 1;
INSERT INTO t_deterministic VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

CREATE TABLE t_bare (s String)
ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO t_bare VALUES ('abc'), ('abc\0'), ('abc\0'), ('b');

-- { echo }

SELECT count() FROM t_monotonic WHERE s <= toFixedString('abc', 4);
SELECT count() FROM t_deterministic WHERE s = toFixedString('abc', 4);
SELECT count() FROM t_deterministic WHERE s IN (SELECT toFixedString('abc', 4));
SELECT count() FROM t_bare WHERE s IN (SELECT toFixedString('abc', 4));
SELECT count() FROM t_bare WHERE s IN (SELECT toNullable(toFixedString('abc', 4)));

DROP TABLE t_monotonic;
DROP TABLE t_deterministic;
DROP TABLE t_bare;
