-- Tuple-element TTL source columns are represented by their enclosing tuple during
-- syntax analysis. The nested `DateTime` element must still be widened before the
-- expression is rebuilt, otherwise this interval wraps in 32 bits and drops the part.
DROP TABLE IF EXISTS t_ttl_tuple_datetime_overflow;
CREATE TABLE t_ttl_tuple_datetime_overflow
(
    t Tuple(id UInt64, ts DateTime),
    value UInt64
)
ENGINE = MergeTree
ORDER BY value;

INSERT INTO t_ttl_tuple_datetime_overflow VALUES ((1, '2034-01-01 00:00:00'), 10), ((2, '2034-06-15 12:00:00'), 20);
SET mutations_sync = 2;
ALTER TABLE t_ttl_tuple_datetime_overflow MODIFY TTL t.ts + INTERVAL 100 YEAR;
SELECT count() FROM t_ttl_tuple_datetime_overflow;
DROP TABLE t_ttl_tuple_datetime_overflow;
