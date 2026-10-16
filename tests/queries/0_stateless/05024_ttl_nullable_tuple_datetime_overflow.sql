-- A tuple-element TTL can use a nullable tuple as its syntax-level source column.
-- The temporal element must be widened through the `Nullable` wrapper.
-- A TTL expression cannot be `Nullable` itself, hence `assumeNotNull`.
SET enable_nullable_tuple_type = 1;
DROP TABLE IF EXISTS t_ttl_nullable_tuple_datetime_overflow;
CREATE TABLE t_ttl_nullable_tuple_datetime_overflow
(
    t Nullable(Tuple(id UInt64, ts DateTime)),
    value UInt64
)
ENGINE = MergeTree
ORDER BY value;

INSERT INTO t_ttl_nullable_tuple_datetime_overflow VALUES ((1, '2034-01-01 00:00:00'), 10), ((2, '2034-06-15 12:00:00'), 20), (NULL, 30);
SET mutations_sync = 2;
ALTER TABLE t_ttl_nullable_tuple_datetime_overflow MODIFY TTL assumeNotNull(t.ts) + INTERVAL 100 YEAR;
SELECT count() FROM t_ttl_nullable_tuple_datetime_overflow;
DROP TABLE t_ttl_nullable_tuple_datetime_overflow;
