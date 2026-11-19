-- { echo }

-- accurateCast/accurateCastOrNull from an unsigned integer to IPv4/IPv6 must convert whenever the plain CAST does.
SELECT accurateCastOrNull(CAST(16909060, 'UInt32'), 'IPv4');
SELECT accurateCast(CAST(16909060, 'UInt32'), 'IPv4');
SELECT accurateCastOrNull(CAST(7, 'UInt8'), 'IPv4');
SELECT accurateCastOrNull(CAST(258, 'UInt16'), 'IPv4');
SELECT accurateCastOrNull(CAST(16909060, 'UInt64'), 'IPv4');
SELECT accurateCastOrNull(CAST(true, 'Bool'), 'IPv4');
SELECT accurateCastOrNull(CAST(1, 'UInt128'), 'IPv6');
SELECT accurateCast(CAST(1, 'UInt128'), 'IPv6');
SELECT accurateCastOrDefault(CAST(16909060, 'UInt32'), 'IPv4');

-- Out of range must be rejected, never truncated to 0.0.0.0 the way the plain CAST does.
SELECT accurateCastOrNull(CAST(4294967295, 'UInt64'), 'IPv4');
SELECT accurateCastOrNull(CAST(4294967296, 'UInt64'), 'IPv4');
SELECT accurateCast(CAST(4294967296, 'UInt64'), 'IPv4'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(CAST(4294967296, 'UInt64'), 'IPv4');

-- The fix must not widen beyond the pairs the plain CAST supports.
SELECT accurateCastOrNull(CAST(1, 'Int32'), 'IPv4');
SELECT accurateCastOrNull(CAST(1, 'Float64'), 'IPv4');
SELECT accurateCastOrNull(CAST(1, 'UInt256'), 'IPv4');
SELECT accurateCastOrNull(CAST(1, 'UInt128'), 'UUID');

-- IPv6 is stored big-endian, so assert the representation is unchanged.
SELECT toIPv6(toUInt128(1)) = accurateCastOrNull(toUInt128(1), 'IPv6');

-- Type wrappers.
SELECT accurateCastOrNull(CAST(16909060, 'Nullable(UInt32)'), 'IPv4');
SELECT accurateCastOrNull(CAST(16909060, 'LowCardinality(UInt32)'), 'IPv4') SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT accurateCastOrNull(x, 'IPv4') FROM (SELECT arrayJoin([toUInt32(0), toUInt32(16909060), toUInt32(4294967295)]) AS x);

-- Runtime IN over a cross-type set, with the equality oracle in the same query.
SELECT CAST(16909060, 'UInt32') IN (SELECT toIPv4('1.2.3.4')), CAST(16909060, 'UInt32') = toIPv4('1.2.3.4');
SELECT CAST(16909060, 'UInt32') IN (SELECT toIPv4('1.2.3.4')) SETTINGS transform_null_in = 1;

-- The MergeTree set index must return the matching row AND still prune.
CREATE TABLE ip_key (a IPv4) ENGINE = MergeTree ORDER BY a PARTITION BY a;
INSERT INTO ip_key VALUES ('1.2.3.4'), ('1.2.3.5'), ('8.8.8.8');
SELECT a FROM ip_key WHERE a IN (SELECT CAST(16909060, 'Nullable(UInt32)') UNION ALL SELECT NULL) SETTINGS transform_null_in = 1;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT a FROM ip_key WHERE a IN (SELECT CAST(16909060, 'Nullable(UInt32)') UNION ALL SELECT NULL) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Parts: 1/3%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT a FROM ip_key WHERE a IN (SELECT CAST(16909060, 'Nullable(UInt32)') UNION ALL SELECT NULL) SETTINGS transform_null_in = 1) WHERE explain LIKE '%0-element set%';
DROP TABLE ip_key;
