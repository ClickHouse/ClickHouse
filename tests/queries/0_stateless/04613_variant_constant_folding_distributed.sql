-- Folded constants of Variant type must survive serialization to a secondary server.
-- https://github.com/ClickHouse/ClickHouse/issues/74366

SELECT 42::UInt64::Variant(UInt64, String) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT 'Hello'::Variant(UInt64, String) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT NULL::Variant(UInt64, String) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;

-- A Variant member with a custom type name: the literal of a `Point` value is a plain tuple,
-- whose type would be inferred back as `Tuple(Float64, Float64)` without the inner cast.
SELECT (0., 0.)::Point::Geometry FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;

-- Comparison of `Variant(String, UInt64)` values throws under the strict comparison behavior
-- (see 02990_variant_where_cond), so use a Variant whose members have a common supertype.
DROP TABLE IF EXISTS t_variant_const_fold;
CREATE TABLE t_variant_const_fold (v Variant(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_const_fold VALUES (42::UInt64), (7::UInt64), (NULL);

SELECT v FROM remote('127.0.0.1', currentDatabase(), t_variant_const_fold)
WHERE v = 42::UInt64::Variant(UInt64)
SETTINGS prefer_localhost_replica = 0;

DROP TABLE t_variant_const_fold;
