-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

-- Part of the 04549/04552-04562 family: one set-index exactness suite split across files to fit
-- the flaky check's 180s per-test budget. Every part is self-contained.

SET explain_query_plan_default = 'legacy';
SET optimize_use_implicit_projections = 0;
-- A randomized `compatibility` below 25.12 reverts this setting to false, and the `Time64` cells then
-- fail to create their column. A session `SET` survives that: the compatibility pass skips settings
-- already changed manually.
SET enable_time_time64_type = 1;
-- The set elements below that spell `DateTime` without a zone take it from the session, which the test
-- runner randomizes; pin it so the no-zone/zone pair stays the discriminator by construction.
SET session_timezone = 'UTC';

-- A set-index atom may only be treated as an exact image of the predicate when the conversion
-- preserves equality in BOTH directions: index preparation casts the set values into the key type,
-- runtime membership casts the key into the set type. Every carrier below returned a WRONG result
-- (rows silently vanished) because a non-equality-preserving cast was treated as exact. Each carrier
-- asserts the MergeTree answer against an identical `ENGINE = Memory` oracle.

SELECT '--- superset carriers (NOT IN over-prunes a live partition) ---';

DROP TABLE IF EXISTS c_a; DROP TABLE IF EXISTS o_a;
CREATE TABLE c_a (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_a (k UInt64) ENGINE = Memory;
INSERT INTO c_a VALUES (1), (2);
INSERT INTO o_a VALUES (1), (2);
SELECT 'A UInt64/String 01',
    (SELECT count() FROM c_a WHERE k IN (SELECT '01')) = (SELECT count() FROM o_a WHERE k IN (SELECT '01')),
    (SELECT count() FROM c_a WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM o_a WHERE k NOT IN (SELECT '01'));
SELECT 'A Nullable element',
    (SELECT count() FROM c_a WHERE k NOT IN (SELECT CAST('01', 'Nullable(String)'))) = (SELECT count() FROM o_a WHERE k NOT IN (SELECT CAST('01', 'Nullable(String)')));

DROP TABLE IF EXISTS c_e; DROP TABLE IF EXISTS o_e;
CREATE TABLE c_e (k Decimal(10, 2)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_e (k Decimal(10, 2)) ENGINE = Memory;
INSERT INTO c_e VALUES (1.00), (2.00);
INSERT INTO o_e VALUES (1.00), (2.00);
SELECT 'E Decimal(10,2)/String 1.001',
    (SELECT count() FROM c_e WHERE k IN (SELECT '1.001')) = (SELECT count() FROM o_e WHERE k IN (SELECT '1.001')),
    (SELECT count() FROM c_e WHERE k NOT IN (SELECT '1.001')) = (SELECT count() FROM o_e WHERE k NOT IN (SELECT '1.001'));

DROP TABLE IF EXISTS c_g; DROP TABLE IF EXISTS o_g;
CREATE TABLE c_g (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_g (k UInt64) ENGINE = Memory;
INSERT INTO c_g VALUES (1), (2);
INSERT INTO o_g VALUES (1), (2);
SELECT 'G UInt64/Decimal64(1) 1.5',
    (SELECT count() FROM c_g WHERE k IN (SELECT toDecimal64(1.5, 1))) = (SELECT count() FROM o_g WHERE k IN (SELECT toDecimal64(1.5, 1))),
    (SELECT count() FROM c_g WHERE k NOT IN (SELECT toDecimal64(1.5, 1))) = (SELECT count() FROM o_g WHERE k NOT IN (SELECT toDecimal64(1.5, 1)));

DROP TABLE IF EXISTS c_k; DROP TABLE IF EXISTS o_k;
CREATE TABLE c_k (k Date) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_k (k Date) ENGINE = Memory;
INSERT INTO c_k VALUES ('2024-01-01'), ('2024-01-02');
INSERT INTO o_k VALUES ('2024-01-01'), ('2024-01-02');
SELECT 'K Date/String 2024-1-1',
    (SELECT count() FROM c_k WHERE k IN (SELECT '2024-1-1')) = (SELECT count() FROM o_k WHERE k IN (SELECT '2024-1-1')),
    (SELECT count() FROM c_k WHERE k NOT IN (SELECT '2024-1-1')) = (SELECT count() FROM o_k WHERE k NOT IN (SELECT '2024-1-1'));
SELECT 'K Nullable element',
    (SELECT count() FROM c_k WHERE k NOT IN (SELECT CAST('2024-1-1', 'Nullable(String)'))) = (SELECT count() FROM o_k WHERE k NOT IN (SELECT CAST('2024-1-1', 'Nullable(String)')));

DROP TABLE IF EXISTS c_l; DROP TABLE IF EXISTS o_l;
CREATE TABLE c_l (k UUID) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_l (k UUID) ENGINE = Memory;
INSERT INTO c_l VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), ('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
INSERT INTO o_l VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), ('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT 'L UUID/uppercase String',
    (SELECT count() FROM c_l WHERE k IN (SELECT '61F0C404-5CB3-11E7-907B-A6006AD3DBA0')) = (SELECT count() FROM o_l WHERE k IN (SELECT '61F0C404-5CB3-11E7-907B-A6006AD3DBA0')),
    (SELECT count() FROM c_l WHERE k NOT IN (SELECT '61F0C404-5CB3-11E7-907B-A6006AD3DBA0')) = (SELECT count() FROM o_l WHERE k NOT IN (SELECT '61F0C404-5CB3-11E7-907B-A6006AD3DBA0'));

DROP TABLE IF EXISTS c_m; DROP TABLE IF EXISTS o_m;
CREATE TABLE c_m (k Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_m (k Enum8('a' = 1, 'b' = 2)) ENGINE = Memory;
INSERT INTO c_m VALUES ('a'), ('b');
INSERT INTO o_m VALUES ('a'), ('b');
SELECT 'M Enum8/String 1',
    (SELECT count() FROM c_m WHERE k IN (SELECT '1')) = (SELECT count() FROM o_m WHERE k IN (SELECT '1')),
    (SELECT count() FROM c_m WHERE k NOT IN (SELECT '1')) = (SELECT count() FROM o_m WHERE k NOT IN (SELECT '1'));

DROP TABLE IF EXISTS c_p; DROP TABLE IF EXISTS o_p;
CREATE TABLE c_p (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE o_p (a UInt64, b UInt64) ENGINE = Memory;
INSERT INTO c_p VALUES (1, 1), (1, 2);
INSERT INTO o_p VALUES (1, 1), (1, 2);
SELECT 'P (UInt64,UInt64)/(String,String)',
    (SELECT count() FROM c_p WHERE (a, b) IN (SELECT ('01', '01'))) = (SELECT count() FROM o_p WHERE (a, b) IN (SELECT ('01', '01'))),
    (SELECT count() FROM c_p WHERE (a, b) NOT IN (SELECT ('01', '01'))) = (SELECT count() FROM o_p WHERE (a, b) NOT IN (SELECT ('01', '01')));

DROP TABLE IF EXISTS c_r; DROP TABLE IF EXISTS o_r;
CREATE TABLE c_r (k Decimal(10, 2)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_r (k Decimal(10, 2)) ENGINE = Memory;
INSERT INTO c_r VALUES (1.00), (2.00);
INSERT INTO o_r VALUES (1.00), (2.00);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'R Decimal(10,2)/Decimal(20,4) 1.0010' AS c1,
    (SELECT count() FROM c_r WHERE k IN (SELECT CAST('1.0010', 'Decimal(20,4)'))) = (SELECT count() FROM o_r WHERE k IN (SELECT CAST('1.0010', 'Decimal(20,4)'))) AS c2,
    (SELECT count() FROM c_r WHERE k NOT IN (SELECT CAST('1.0010', 'Decimal(20,4)'))) = (SELECT count() FROM o_r WHERE k NOT IN (SELECT CAST('1.0010', 'Decimal(20,4)'))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'S Decimal(10,2)/Decimal(10,4) 1.0010' AS c1,
    (SELECT count() FROM c_r WHERE k IN (SELECT CAST('1.0010', 'Decimal(10,4)'))) = (SELECT count() FROM o_r WHERE k IN (SELECT CAST('1.0010', 'Decimal(10,4)'))) AS c2,
    (SELECT count() FROM c_r WHERE k NOT IN (SELECT CAST('1.0010', 'Decimal(10,4)'))) = (SELECT count() FROM o_r WHERE k NOT IN (SELECT CAST('1.0010', 'Decimal(10,4)'))) AS c3
) ORDER BY ord;

DROP TABLE IF EXISTS c_t; DROP TABLE IF EXISTS o_t;
CREATE TABLE c_t (k DateTime64(3)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_t (k DateTime64(3)) ENGINE = Memory;
INSERT INTO c_t VALUES ('2024-01-01 00:00:00.123'), ('2024-01-01 00:00:00.124');
INSERT INTO o_t VALUES ('2024-01-01 00:00:00.123'), ('2024-01-01 00:00:00.124');
SELECT 'T DateTime64(3)/DateTime64(6)',
    (SELECT count() FROM c_t WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123456', 'DateTime64(6)'))) = (SELECT count() FROM o_t WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123456', 'DateTime64(6)'))),
    (SELECT count() FROM c_t WHERE k NOT IN (SELECT CAST('2024-01-01 00:00:00.123456', 'DateTime64(6)'))) = (SELECT count() FROM o_t WHERE k NOT IN (SELECT CAST('2024-01-01 00:00:00.123456', 'DateTime64(6)')));

DROP TABLE IF EXISTS c_u; DROP TABLE IF EXISTS o_u;
CREATE TABLE c_u (k DateTime) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_u (k DateTime) ENGINE = Memory;
INSERT INTO c_u VALUES ('2024-01-01 00:00:00'), ('2024-01-01 00:00:01');
INSERT INTO o_u VALUES ('2024-01-01 00:00:00'), ('2024-01-01 00:00:01');
SELECT 'U DateTime/DateTime64(3)',
    (SELECT count() FROM c_u WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)'))) = (SELECT count() FROM o_u WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)'))),
    (SELECT count() FROM c_u WHERE k NOT IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)'))) = (SELECT count() FROM o_u WHERE k NOT IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)')));

DROP TABLE IF EXISTS c_v; DROP TABLE IF EXISTS o_v;
CREATE TABLE c_v (k Int64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_v (k Int64) ENGINE = Memory;
INSERT INTO c_v VALUES (1), (2);
INSERT INTO o_v VALUES (1), (2);
SELECT 'V Int64/Decimal(10,2) 1.50',
    (SELECT count() FROM c_v WHERE k IN (SELECT CAST('1.50', 'Decimal(10,2)'))) = (SELECT count() FROM o_v WHERE k IN (SELECT CAST('1.50', 'Decimal(10,2)'))),
    (SELECT count() FROM c_v WHERE k NOT IN (SELECT CAST('1.50', 'Decimal(10,2)'))) = (SELECT count() FROM o_v WHERE k NOT IN (SELECT CAST('1.50', 'Decimal(10,2)')));

DROP TABLE IF EXISTS c_y; DROP TABLE IF EXISTS o_y;
CREATE TABLE c_y (k UInt8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_y (k UInt8) ENGINE = Memory;
INSERT INTO c_y VALUES (1), (2), (3);
INSERT INTO o_y VALUES (1), (2), (3);
SELECT 'Y UInt8/Enum16 a=257',
    (SELECT count() FROM c_y WHERE k IN (SELECT CAST('a', 'Enum16(\'a\' = 257)'))) = (SELECT count() FROM o_y WHERE k IN (SELECT CAST('a', 'Enum16(\'a\' = 257)'))),
    (SELECT count() FROM c_y WHERE k NOT IN (SELECT CAST('a', 'Enum16(\'a\' = 257)'))) = (SELECT count() FROM o_y WHERE k NOT IN (SELECT CAST('a', 'Enum16(\'a\' = 257)')));

DROP TABLE IF EXISTS c_z; DROP TABLE IF EXISTS o_z;
CREATE TABLE c_z (k DateTime64(2)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_z (k DateTime64(2)) ENGINE = Memory;
-- The first row must be the value the Decimal element maps to, or nothing can be lost.
INSERT INTO c_z VALUES ('1970-01-01 00:00:01.50'), ('1970-01-01 00:00:02.50');
INSERT INTO o_z VALUES ('1970-01-01 00:00:01.50'), ('1970-01-01 00:00:02.50');
SELECT 'Z DateTime64(2)/Decimal(10,2) 1.50',
    (SELECT count() FROM c_z WHERE k IN (SELECT CAST('1.50', 'Decimal(10,2)'))) = (SELECT count() FROM o_z WHERE k IN (SELECT CAST('1.50', 'Decimal(10,2)'))),
    (SELECT count() FROM c_z WHERE k NOT IN (SELECT CAST('1.50', 'Decimal(10,2)'))) = (SELECT count() FROM o_z WHERE k NOT IN (SELECT CAST('1.50', 'Decimal(10,2)')));

DROP TABLE IF EXISTS c_ab; DROP TABLE IF EXISTS o_ab;
CREATE TABLE c_ab (k Date) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_ab (k Date) ENGINE = Memory;
INSERT INTO c_ab VALUES ('1970-01-01'), ('1970-01-02');
INSERT INTO o_ab VALUES ('1970-01-01'), ('1970-01-02');
SELECT 'AB Date/Date32 1969-12-31',
    (SELECT count() FROM c_ab WHERE k IN (SELECT toDate32('1969-12-31'))) = (SELECT count() FROM o_ab WHERE k IN (SELECT toDate32('1969-12-31'))),
    (SELECT count() FROM c_ab WHERE k NOT IN (SELECT toDate32('1969-12-31'))) = (SELECT count() FROM o_ab WHERE k NOT IN (SELECT toDate32('1969-12-31')));

SELECT '--- under-approximating carriers (IN over-prunes a live partition) ---';

DROP TABLE IF EXISTS c_d; DROP TABLE IF EXISTS o_d;
CREATE TABLE c_d (k String) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_d (k String) ENGINE = Memory;
INSERT INTO c_d VALUES ('1'), ('01');
INSERT INTO o_d VALUES ('1'), ('01');
SELECT 'D String/UInt8 1',
    (SELECT count() FROM c_d WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM o_d WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM c_d WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM o_d WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'D Nullable element',
    (SELECT count() FROM c_d WHERE k IN (SELECT CAST(1, 'Nullable(UInt8)'))) = (SELECT count() FROM o_d WHERE k IN (SELECT CAST(1, 'Nullable(UInt8)')));

DROP TABLE IF EXISTS c_f; DROP TABLE IF EXISTS o_f;
CREATE TABLE c_f (k DateTime64(6)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_f (k DateTime64(6)) ENGINE = Memory;
INSERT INTO c_f VALUES ('2024-01-01 00:00:00.123456'), ('2024-01-01 00:00:00.123999');
INSERT INTO o_f VALUES ('2024-01-01 00:00:00.123456'), ('2024-01-01 00:00:00.123999');
SELECT 'F DateTime64(6)/DateTime64(3)',
    (SELECT count() FROM c_f WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)'))) = (SELECT count() FROM o_f WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)'))),
    (SELECT count() FROM c_f WHERE k NOT IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)'))) = (SELECT count() FROM o_f WHERE k NOT IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)')));
SELECT 'F Nullable element',
    (SELECT count() FROM c_f WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123', 'Nullable(DateTime64(3))'))) = (SELECT count() FROM o_f WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123', 'Nullable(DateTime64(3))')));

DROP TABLE IF EXISTS c_h; DROP TABLE IF EXISTS o_h;
CREATE TABLE c_h (k Decimal(20, 0)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_h (k Decimal(20, 0)) ENGINE = Memory;
INSERT INTO c_h VALUES (16777216), (16777217);
INSERT INTO o_h VALUES (16777216), (16777217);
SELECT 'H Decimal(20,0)/Float32 16777216',
    (SELECT count() FROM c_h WHERE k IN (SELECT toFloat32(16777216))) = (SELECT count() FROM o_h WHERE k IN (SELECT toFloat32(16777216))),
    (SELECT count() FROM c_h WHERE k NOT IN (SELECT toFloat32(16777216))) = (SELECT count() FROM o_h WHERE k NOT IN (SELECT toFloat32(16777216)));

DROP TABLE IF EXISTS c_i; DROP TABLE IF EXISTS o_i;
CREATE TABLE c_i (k Decimal(20, 0)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_i (k Decimal(20, 0)) ENGINE = Memory;
INSERT INTO c_i VALUES (9007199254740992), (9007199254740993);
INSERT INTO o_i VALUES (9007199254740992), (9007199254740993);
SELECT 'I Decimal(20,0)/Float64 9007199254740992',
    (SELECT count() FROM c_i WHERE k IN (SELECT toFloat64(9007199254740992))) = (SELECT count() FROM o_i WHERE k IN (SELECT toFloat64(9007199254740992))),
    (SELECT count() FROM c_i WHERE k NOT IN (SELECT toFloat64(9007199254740992))) = (SELECT count() FROM o_i WHERE k NOT IN (SELECT toFloat64(9007199254740992)));

DROP TABLE IF EXISTS c_j; DROP TABLE IF EXISTS o_j;
CREATE TABLE c_j (k DateTime) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_j (k DateTime) ENGINE = Memory;
INSERT INTO c_j VALUES ('2024-01-01 00:00:00'), ('2024-01-01 12:00:00');
INSERT INTO o_j VALUES ('2024-01-01 00:00:00'), ('2024-01-01 12:00:00');
SELECT 'J DateTime/Date',
    (SELECT count() FROM c_j WHERE k IN (SELECT toDate('2024-01-01'))) = (SELECT count() FROM o_j WHERE k IN (SELECT toDate('2024-01-01'))),
    (SELECT count() FROM c_j WHERE k NOT IN (SELECT toDate('2024-01-01'))) = (SELECT count() FROM o_j WHERE k NOT IN (SELECT toDate('2024-01-01')));
