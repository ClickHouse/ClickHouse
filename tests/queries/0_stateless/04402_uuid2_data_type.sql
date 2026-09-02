-- Tests for the UUID2 data type (correctly-sorting variant of UUID) and the uuid_type_version setting.

SELECT '-- type names and alias';
SELECT toTypeName('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2);
SELECT toTypeName('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID1); -- UUID1 is an alias of UUID
SELECT toTypeName('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID);

SELECT '-- text round-trip is canonical';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 AS x, toString(x);

SELECT '-- UUID2 sorts lexicographically (unlike UUID)';
DROP TABLE IF EXISTS t_uuid2;
CREATE TABLE t_uuid2 (x UUID2) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_uuid2 VALUES ('00000002-0000-0000-0000-000000000000'), ('00000000-0000-0000-0002-000000000000'), ('00000000-0000-0000-0000-000000000009');
SELECT x FROM t_uuid2 ORDER BY x;
SELECT '-- same values as UUID would sort differently';
SELECT x FROM (SELECT arrayJoin(['00000002-0000-0000-0000-000000000000', '00000000-0000-0000-0002-000000000000', '00000000-0000-0000-0000-000000000009'])::UUID AS x) ORDER BY x;

SELECT '-- min/max';
SELECT min(x), max(x) FROM t_uuid2;

SELECT '-- conversions round-trip';
WITH '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 AS u2
SELECT
    u2 = CAST(CAST(u2 AS String) AS UUID2),
    u2 = CAST(CAST(u2 AS UInt128) AS UUID2),
    u2 = CAST(CAST(u2 AS UUID) AS UUID2);

SELECT '-- FixedString(16) (canonical bytes) parses to UUID2';
SELECT CAST(CAST(unhex('61f0c4045cb311e7907ba6006ad3dba0') AS FixedString(16)) AS UUID2);

SELECT '-- UUID2 -> UInt128 (identity) equals UUID -> UInt128 (half-swap): both give the canonical integer';
SELECT CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 AS UInt128) = CAST('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID AS UInt128);

SELECT '-- comparison with string literal';
SELECT count() FROM t_uuid2 WHERE x = '00000000-0000-0000-0000-000000000009';

SELECT '-- Field-level UUID <-> UUID2 coercion swaps halves (convertFieldToType, e.g. IN-set)';
-- A `UUID` constant coerced into a `UUID2` IN-set (and vice versa) must denote the same textual value.
SELECT count() FROM t_uuid2 WHERE x IN (toUUID('00000000-0000-0000-0000-000000000009'));
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 IN (toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) AS uuid_into_uuid2,
       '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID IN ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2) AS uuid2_into_uuid,
       '61f0c404-5cb3-11e7-907b-a6006ad3dba0'::UUID2 IN (toUUID('00000000-0000-0000-0000-000000000001')) AS different_value;

SELECT '-- direct UUID <-> UUID2 comparison (least supertype is UUID2)';
-- `UUID` and `UUID2` denote the same logical values in layouts differing by a half-swap. Their common type
-- is the correctly-sorting `UUID2`, so `=` / `!=` (and `if` / array literals) reconcile them instead of
-- throwing during execution because no common type exists.
WITH '61f0c404-5cb3-11e7-907b-a6006ad3dba0' AS s, '00000000-0000-0000-0000-000000000001' AS s2
SELECT
    s::UUID2 = toUUID(s),
    toUUID2(s) = s::UUID,
    s::UUID2 = toUUID(s2),
    s::UUID2 != toUUID(s2),
    toTypeName(if(1, s::UUID2, toUUID(s))),
    toTypeName([s::UUID2, toUUID(s)]);
SELECT '-- comparison of a UUID2 column against a UUID constant';
SELECT count() FROM t_uuid2 WHERE x = toUUID('00000000-0000-0000-0000-000000000009');

SELECT '-- JOIN between UUID and UUID2 keys (common type is UUID2)';
DROP TABLE IF EXISTS t_join_uuid;
DROP TABLE IF EXISTS t_join_uuid2;
CREATE TABLE t_join_uuid (k UUID, v String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_join_uuid2 (k UUID2, w String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_join_uuid VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'a'), ('00000000-0000-0000-0000-000000000001', 'b');
INSERT INTO t_join_uuid2 VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'x');
SELECT v, w FROM t_join_uuid JOIN t_join_uuid2 ON t_join_uuid.k = t_join_uuid2.k ORDER BY v;

SELECT '-- setting materializes bare UUID as UUID2 (version 2), leaves UUID1/UUID2 explicit';
DROP TABLE IF EXISTS t_mat;
SET uuid_type_version = 2;
CREATE TABLE t_mat (a UUID, b UUID1, c UUID2, d Array(UUID), e Nullable(UUID)) ENGINE = MergeTree ORDER BY tuple();
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_mat' ORDER BY name;
SELECT '-- version 1 (default) leaves UUID as UUID';
SET uuid_type_version = 1;
DROP TABLE IF EXISTS t_mat1;
CREATE TABLE t_mat1 (a UUID, d Array(UUID)) ENGINE = MergeTree ORDER BY tuple();
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_mat1' ORDER BY name;

SELECT '-- ALTER ADD/MODIFY COLUMN materializes bare UUID as UUID2 (version 2), leaves UUID1/UUID2 explicit';
SET uuid_type_version = 2;
DROP TABLE IF EXISTS t_alter;
CREATE TABLE t_alter (id UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_alter ADD COLUMN a UUID, ADD COLUMN b UUID1, ADD COLUMN c UUID2, ADD COLUMN d Array(UUID), ADD COLUMN e Nullable(UUID);
ALTER TABLE t_alter MODIFY COLUMN b UUID;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter' ORDER BY name;
SELECT '-- ALTER version 1 (default) leaves UUID as UUID';
SET uuid_type_version = 1;
ALTER TABLE t_alter ADD COLUMN f UUID, ADD COLUMN g Array(UUID);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter' AND name IN ('f', 'g') ORDER BY name;

SELECT '-- CREATE ON CLUSTER with a legacy DDL format version still materializes bare UUID as UUID2 on the initiator';
SET distributed_ddl_output_mode = 'none';
-- A version below NORMALIZE_CREATE_ON_INITIATOR_VERSION (3) takes the legacy path that enqueues the query before it is
-- normalized in `createTable`; the initiator must still bake in `uuid_type_version` so workers do not fall back to `UUID`.
SET distributed_ddl_entry_format_version = 2;
SET uuid_type_version = 2;
DROP TABLE IF EXISTS t_cluster_legacy ON CLUSTER test_shard_localhost SYNC;
CREATE TABLE t_cluster_legacy ON CLUSTER test_shard_localhost (a UUID, b UUID1, c UUID2, d Array(UUID), e Nullable(UUID)) ENGINE = MergeTree ORDER BY tuple();
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_cluster_legacy' ORDER BY name;
DROP TABLE IF EXISTS t_cluster_legacy ON CLUSTER test_shard_localhost SYNC;
SET distributed_ddl_entry_format_version = DEFAULT;
SET distributed_ddl_output_mode = DEFAULT;
SET uuid_type_version = 1;

SELECT '-- function parity: hex/UUIDv7ToDateTime/reinterpret/empty match UUID for the same value';
WITH '0192d2b8-7c3f-7e1a-b2c4-1234567890ab' AS s
SELECT
    hex(s::UUID2) = hex(s::UUID),
    UUIDToNum(s::UUID2) = UUIDToNum(s::UUID),
    UUIDv7ToDateTime('0192d2b8-7c3f-7e1a-b2c4-1234567890ab'::UUID2) = UUIDv7ToDateTime('0192d2b8-7c3f-7e1a-b2c4-1234567890ab'::UUID),
    reinterpretAsUInt128('00000000-0000-0000-0000-000000000001'::UUID2),
    empty('00000000-0000-0000-0000-000000000000'::UUID2),
    notEmpty(s::UUID2);

SELECT '-- hashing parity: hashes match UUID for the same value';
-- `halfMD5` is omitted on purpose: it requires SSL and is absent from the fast-test build.
-- It is a `FunctionAnyHash` like the hashes below, so it exercises the same `UUID2` code path.
WITH '0192d2b8-7c3f-7e1a-b2c4-1234567890ab' AS s
SELECT
    sipHash64(s::UUID2) = sipHash64(s::UUID),
    sipHash128(s::UUID2) = sipHash128(s::UUID),
    cityHash64(s::UUID2) = cityHash64(s::UUID),
    xxHash64(s::UUID2) = xxHash64(s::UUID),
    farmHash64(s::UUID2) = farmHash64(s::UUID);

SELECT '-- mapAdd / mapSubtract on Map(UUID2, ...) keys (nested UUID materialization parity)';
-- `uuid_type_version = 2` recursively rewrites nested `UUID` to `UUID2`, so `mapAdd` / `mapSubtract`
-- must dispatch `UUID2` map keys the same way as `UUID`; the result key type is preserved verbatim.
WITH '61f0c404-5cb3-11e7-907b-a6006ad3dba0' AS s
SELECT
    toTypeName(mapAdd(map(s::UUID2, 1::UInt64), map(s::UUID2, 2::UInt64))),
    mapAdd(map(s::UUID2, 1::UInt64), map(s::UUID2, 2::UInt64))[s::UUID2],
    mapSubtract(map(s::UUID2, 5::UInt64), map(s::UUID2, 2::UInt64))[s::UUID2];

SELECT '-- -Map aggregate combinator on Map(UUID2, ...) keys';
-- `uuid_type_version = 2` materializes nested `UUID` map keys to `UUID2`, so the `-Map` aggregate
-- combinator must dispatch `UUID2` keys the same way as `UUID` and preserve the key type in the result.
WITH '61f0c404-5cb3-11e7-907b-a6006ad3dba0' AS s1, '00000000-0000-0000-0000-000000000001' AS s2
SELECT
    toTypeName(sumMap(m)),
    sumMap(m)[s1::UUID2],
    sumMap(m)[s2::UUID2]
FROM (SELECT map(s1::UUID2, 1::UInt64, s2::UUID2, 10::UInt64) AS m FROM numbers(3));

SELECT '-- uniq* parity: UUID2 takes the same fixed-width path as UUID';
-- `UUID2` shares `ColumnVector<UUID>` with `UUID`, so the single-argument `uniq*` factories route it through the
-- fixed-width fast path instead of the generic variadic hash. The exact variants (`uniqExact`, `uniqUpTo`) match
-- `UUID` exactly because the layout change is a bijection; `uniq` and `uniqCombined` are exact at this cardinality
-- and match too. `uniqHLL12` is a pure HyperLogLog estimate over the physical bytes, so it is only checked to agree
-- with the `UUID` estimate within its error bound.
SELECT
    uniqExact(u::UUID2) = uniqExact(u::UUID),
    uniq(u::UUID2) = uniq(u::UUID),
    uniqCombined(u::UUID2) = uniqCombined(u::UUID),
    uniqUpTo(100)(u::UUID2) = uniqUpTo(100)(u::UUID),
    uniqExact(u::UUID2) = 100,
    abs(toInt64(uniqHLL12(u::UUID2)) - toInt64(uniqHLL12(u::UUID))) <= 5
FROM (SELECT reinterpretAsUUID(toUInt128(number)) AS u FROM numbers(100));

SELECT '-- generateRandom produces UUID2';
SELECT count() FROM (SELECT * FROM generateRandom('x UUID2', 1, 1, 1) LIMIT 5);

SELECT '-- bloom_filter skip index on UUID2';
DROP TABLE IF EXISTS t_bf;
CREATE TABLE t_bf (x UUID2, INDEX idx x TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT count() FROM t_bf WHERE x = '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT count() FROM t_bf WHERE x = '00000000-0000-0000-0000-000000000000';

DROP TABLE t_uuid2;
DROP TABLE t_mat;
DROP TABLE t_mat1;
DROP TABLE t_alter;
DROP TABLE t_bf;
DROP TABLE t_join_uuid;
DROP TABLE t_join_uuid2;
