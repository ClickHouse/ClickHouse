DROP TABLE IF EXISTS t_enum_datetime;
SET session_timezone = 'UTC';

-- An `Enum8`/`Enum16` source converts to `DateTime` and `Date` with the same overflow handling as the
-- `Int8`/`Int16` its value is stored as: it saturates instead of wrapping.

SELECT toDateTime(CAST('neg', $$Enum8('neg' = -1, 'zero' = 0)$$)), toDateTime(toInt8(-1));
SELECT toDateTime(CAST('lo', $$Enum16('lo' = -32768, 'hi' = 32767)$$)), toDateTime(toInt16(-32768));
SELECT toDate(CAST('neg', $$Enum8('neg' = -1, 'zero' = 0)$$)), toDate(toInt8(-1));
SELECT toDate(CAST('lo', $$Enum16('lo' = -32768, 'hi' = 32767)$$)), toDate(toInt16(-32768));

-- In-range values are unaffected.
SELECT toDateTime(CAST('hi', $$Enum16('lo' = -32768, 'hi' = 32767)$$)), toDateTime(toInt16(32767));
SELECT toDate(CAST('hi', $$Enum16('lo' = -32768, 'hi' = 32767)$$)), toDate(toInt16(32767));

-- `CAST` takes the same route, and neither a `Nullable` nor a non-constant argument changes the answer.
SELECT CAST(CAST('neg', $$Enum8('neg' = -1, 'zero' = 0)$$) AS DateTime),
       toDateTime(toNullable(CAST('neg', $$Enum8('neg' = -1, 'zero' = 0)$$))),
       toDateTime(materialize(CAST('neg', $$Enum8('neg' = -1, 'zero' = 0)$$)));

-- The behaviour agrees with the `Int8` source in every overflow mode, including `throw`, which these
-- numeric conversions do not consult (a separate, pre-existing gap).
SELECT toDateTime(CAST('neg', $$Enum8('neg' = -1, 'zero' = 0)$$)), toDateTime(toInt8(-1)) SETTINGS date_time_overflow_behavior = 'throw';
SELECT toDate(CAST('neg', $$Enum8('neg' = -1, 'zero' = 0)$$)), toDate(toInt8(-1)) SETTINGS date_time_overflow_behavior = 'saturate';

-- The conversion is used for primary-key analysis, and a wrapped key boundary inverted the range: the
-- granule holding the matching row was discarded, and the set-index check aborted a debug build with
-- `Invalid binary search result in MergeTreeSetIndex`. The second column of the first row counts the
-- same predicate without the index, so a range that is pruned again cannot pass unnoticed.

CREATE TABLE t_enum_datetime (x Enum16('neg' = -1, 'zero' = 0)) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_enum_datetime VALUES ('neg'), ('zero');

SELECT (SELECT count() FROM t_enum_datetime WHERE toDateTime(x) = toDateTime(0)),
       (SELECT countIf(toDateTime(x) = toDateTime(0)) FROM t_enum_datetime);
SELECT count() FROM t_enum_datetime WHERE toDateTime(x) IN (toDateTime(0), toDateTime(1));
SELECT count() FROM t_enum_datetime WHERE toDate(x) IN (toDate(0), toDate(1));

DROP TABLE t_enum_datetime;
