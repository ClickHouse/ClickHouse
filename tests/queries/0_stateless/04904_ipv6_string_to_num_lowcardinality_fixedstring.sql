-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/80263
-- `IPv6StringToNum` accepts `FixedString` arguments, but its `LowCardinality` fast path used to throw
-- `Illegal column type FixedString. Expected String` for a `LowCardinality(FixedString)` dictionary.
-- The fast path now falls back to the regular path, which handles every supported argument type.

SELECT hex(IPv6StringToNum(toLowCardinality(toFixedString('::1', 3))));
SELECT hex(IPv6StringToNum(toLowCardinality(toFixedString('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D', 39))));
SELECT toTypeName(IPv6StringToNumOrNull(toLowCardinality(toFixedString('::1', 3))));
SELECT IPv6StringToNumOrNull(toLowCardinality(toFixedString('bad', 3)));
SELECT hex(IPv6StringToNum(toLowCardinality(toFixedString('bad', 3)))) SETTINGS cast_ipv4_ipv6_default_on_conversion_error = 1;

-- The result must match the plain (non-LowCardinality) FixedString path.
SELECT hex(IPv6StringToNum(toLowCardinality(toFixedString('::1', 3)))) = hex(IPv6StringToNum(toFixedString('::1', 3)));
