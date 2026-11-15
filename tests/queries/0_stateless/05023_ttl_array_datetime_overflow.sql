-- An `Array(DateTime)` source is represented by the enclosing array during syntax
-- analysis. Widen its nested timestamp before evaluating the interval so it cannot wrap.
DROP TABLE IF EXISTS t_ttl_array_datetime_overflow;
CREATE TABLE t_ttl_array_datetime_overflow
(
    arr Array(DateTime),
    value UInt64
)
ENGINE = MergeTree
ORDER BY value;

INSERT INTO t_ttl_array_datetime_overflow VALUES (['2034-01-01 00:00:00'], 10), (['2034-06-15 12:00:00'], 20);
SET mutations_sync = 2;
ALTER TABLE t_ttl_array_datetime_overflow MODIFY TTL arr[1] + INTERVAL 100 YEAR;
SELECT count() FROM t_ttl_array_datetime_overflow;
DROP TABLE t_ttl_array_datetime_overflow;
