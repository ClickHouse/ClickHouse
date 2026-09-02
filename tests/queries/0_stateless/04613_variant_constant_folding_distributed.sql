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

-- Nested inside a compound constant the member type has to be named per element, not once for the
-- whole constant.
SELECT [42::UInt64]::Array(Variant(UInt64, String)) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT tuple(42::UInt64::Variant(UInt64, String)) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT map('k', 42::UInt64::Variant(UInt64, String)) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT [[(0., 0.)::Point::Geometry]] FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;

-- A `DateTime` member is exact only as its raw Unix timestamp: both epochs below format to the local
-- text `2023-10-29 02:10:00` in the DST overlap, so the text form comes back an hour early.
SELECT arrayMap(x -> toUnixTimestamp(assumeNotNull(variantElement(x, 'DateTime(\'Europe/Berlin\')'))), [toDateTime(1698541800, 'Europe/Berlin')::Variant(DateTime('Europe/Berlin'), String)]) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
