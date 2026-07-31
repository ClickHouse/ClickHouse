-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

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
SELECT 'R Decimal(10,2)/Decimal(20,4) 1.0010',
    (SELECT count() FROM c_r WHERE k IN (SELECT CAST('1.0010', 'Decimal(20,4)'))) = (SELECT count() FROM o_r WHERE k IN (SELECT CAST('1.0010', 'Decimal(20,4)'))),
    (SELECT count() FROM c_r WHERE k NOT IN (SELECT CAST('1.0010', 'Decimal(20,4)'))) = (SELECT count() FROM o_r WHERE k NOT IN (SELECT CAST('1.0010', 'Decimal(20,4)')));
SELECT 'S Decimal(10,2)/Decimal(10,4) 1.0010',
    (SELECT count() FROM c_r WHERE k IN (SELECT CAST('1.0010', 'Decimal(10,4)'))) = (SELECT count() FROM o_r WHERE k IN (SELECT CAST('1.0010', 'Decimal(10,4)'))),
    (SELECT count() FROM c_r WHERE k NOT IN (SELECT CAST('1.0010', 'Decimal(10,4)'))) = (SELECT count() FROM o_r WHERE k NOT IN (SELECT CAST('1.0010', 'Decimal(10,4)')));

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

DROP TABLE IF EXISTS c_n; DROP TABLE IF EXISTS o_n;
CREATE TABLE c_n (k IPv4) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_n (k IPv4) ENGINE = Memory;
INSERT INTO c_n VALUES ('1.2.3.4'), ('5.6.7.8');
INSERT INTO o_n VALUES ('1.2.3.4'), ('5.6.7.8');
SELECT 'N IPv4/UInt32 16909060',
    (SELECT count() FROM c_n WHERE k IN (SELECT toUInt32(16909060))) = (SELECT count() FROM o_n WHERE k IN (SELECT toUInt32(16909060))),
    (SELECT count() FROM c_n WHERE k NOT IN (SELECT toUInt32(16909060))) = (SELECT count() FROM o_n WHERE k NOT IN (SELECT toUInt32(16909060)));
-- The preparation cast NULLed the element, master dropped it and still called the set exact.
SELECT 'N no longer an empty set', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM c_n WHERE k IN (SELECT toUInt32(16909060))) WHERE explain ILIKE '%0-element set%';

DROP TABLE IF EXISTS c_o; DROP TABLE IF EXISTS o_o;
CREATE TABLE c_o (k String) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_o (k String) ENGINE = Memory;
INSERT INTO c_o VALUES ('ab'), ('ab\0');
INSERT INTO o_o VALUES ('ab'), ('ab\0');
SELECT 'O String/FixedString(3) NUL-padded',
    (SELECT count() FROM c_o WHERE k IN (SELECT toFixedString('ab', 3))) = (SELECT count() FROM o_o WHERE k IN (SELECT toFixedString('ab', 3))),
    (SELECT count() FROM c_o WHERE k NOT IN (SELECT toFixedString('ab', 3))) = (SELECT count() FROM o_o WHERE k NOT IN (SELECT toFixedString('ab', 3)));

DROP TABLE IF EXISTS c_q; DROP TABLE IF EXISTS o_q;
CREATE TABLE c_q (k LowCardinality(String)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_q (k LowCardinality(String)) ENGINE = Memory;
INSERT INTO c_q VALUES ('1'), ('01');
INSERT INTO o_q VALUES ('1'), ('01');
SELECT 'Q LowCardinality(String)/UInt8 1',
    (SELECT count() FROM c_q WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM o_q WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM c_q WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM o_q WHERE k NOT IN (SELECT toUInt8(1)));

DROP TABLE IF EXISTS c_w; DROP TABLE IF EXISTS o_w;
CREATE TABLE c_w (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_w (k UInt64) ENGINE = Memory;
-- Both keys match toDate('2024-01-01') at runtime (day number and unix seconds); only the
-- first is in the pruning set, so the second silently vanishes.
INSERT INTO c_w VALUES (19723), (1704067200);
INSERT INTO o_w VALUES (19723), (1704067200);
SELECT 'W UInt64/Date',
    (SELECT count() FROM c_w WHERE k IN (SELECT toDate('2024-01-01'))) = (SELECT count() FROM o_w WHERE k IN (SELECT toDate('2024-01-01'))),
    (SELECT count() FROM c_w WHERE k NOT IN (SELECT toDate('2024-01-01'))) = (SELECT count() FROM o_w WHERE k NOT IN (SELECT toDate('2024-01-01')));

DROP TABLE IF EXISTS c_x; DROP TABLE IF EXISTS o_x;
CREATE TABLE c_x (k Int64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_x (k Int64) ENGINE = Memory;
INSERT INTO c_x VALUES (19723), (1704067200);
INSERT INTO o_x VALUES (19723), (1704067200);
SELECT 'X Int64/Date',
    (SELECT count() FROM c_x WHERE k IN (SELECT toDate('2024-01-01'))) = (SELECT count() FROM o_x WHERE k IN (SELECT toDate('2024-01-01'))),
    (SELECT count() FROM c_x WHERE k NOT IN (SELECT toDate('2024-01-01'))) = (SELECT count() FROM o_x WHERE k NOT IN (SELECT toDate('2024-01-01')));

DROP TABLE IF EXISTS c_aa; DROP TABLE IF EXISTS o_aa;
CREATE TABLE c_aa (k UInt8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_aa (k UInt8) ENGINE = Memory;
INSERT INTO c_aa VALUES (1), (2), (3);
INSERT INTO o_aa VALUES (1), (2), (3);
SELECT 'AA UInt8/Bool true',
    (SELECT count() FROM c_aa WHERE k IN (SELECT CAST('true', 'Bool'))) = (SELECT count() FROM o_aa WHERE k IN (SELECT CAST('true', 'Bool'))),
    (SELECT count() FROM c_aa WHERE k NOT IN (SELECT CAST('true', 'Bool'))) = (SELECT count() FROM o_aa WHERE k NOT IN (SELECT CAST('true', 'Bool')));

DROP TABLE IF EXISTS c_ac; DROP TABLE IF EXISTS o_ac;
CREATE TABLE c_ac (k Date32) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_ac (k Date32) ENGINE = Memory;
INSERT INTO c_ac VALUES ('1969-12-31'), ('1970-01-01');
INSERT INTO o_ac VALUES ('1969-12-31'), ('1970-01-01');
SELECT 'AC Date32/Date',
    (SELECT count() FROM c_ac WHERE k IN (SELECT toDate('1970-01-01'))) = (SELECT count() FROM o_ac WHERE k IN (SELECT toDate('1970-01-01'))),
    (SELECT count() FROM c_ac WHERE k NOT IN (SELECT toDate('1970-01-01'))) = (SELECT count() FROM o_ac WHERE k NOT IN (SELECT toDate('1970-01-01')));

DROP TABLE IF EXISTS c_n6; DROP TABLE IF EXISTS o_n6;
CREATE TABLE c_n6 (k Decimal(20, 4)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_n6 (k Decimal(20, 4)) ENGINE = Memory;
INSERT INTO c_n6 VALUES (1.0000), (1.0001);
INSERT INTO o_n6 VALUES (1.0000), (1.0001);
SELECT 'N6 Decimal(20,4)/Decimal(10,2) 1.00',
    (SELECT count() FROM c_n6 WHERE k IN (SELECT CAST('1.00', 'Decimal(10,2)'))) = (SELECT count() FROM o_n6 WHERE k IN (SELECT CAST('1.00', 'Decimal(10,2)'))),
    (SELECT count() FROM c_n6 WHERE k NOT IN (SELECT CAST('1.00', 'Decimal(10,2)'))) = (SELECT count() FROM o_n6 WHERE k NOT IN (SELECT CAST('1.00', 'Decimal(10,2)')));

DROP TABLE IF EXISTS c_p8; DROP TABLE IF EXISTS o_p8;
CREATE TABLE c_p8 (k UUID) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_p8 (k UUID) ENGINE = Memory;
INSERT INTO c_p8 VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0000-0000-000000000002');
INSERT INTO o_p8 VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0000-0000-000000000002');
SELECT 'P8 UUID/UInt128 1',
    (SELECT count() FROM c_p8 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM o_p8 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM c_p8 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM o_p8 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'P8 no longer an empty set', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM c_p8 WHERE k IN (SELECT toUInt128(1))) WHERE explain ILIKE '%0-element set%';

SELECT '--- arm 1: identical types stay exact (no conversion runs) ---';

DROP TABLE IF EXISTS e1_s; CREATE TABLE e1_s (k String) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_s VALUES ('01'), ('02');
SELECT 'arm1 String/String', count() FROM e1_s WHERE k IN (SELECT '01');
SELECT 'arm1 String/String prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_s WHERE k IN (SELECT '01')) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_d; CREATE TABLE e1_d (k Date) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_d VALUES ('2024-01-01'), ('2024-01-02');
SELECT 'arm1 Date/Date', count() FROM e1_d WHERE k IN (SELECT toDate('2024-01-01'));
SELECT 'arm1 Date/Date prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_d WHERE k IN (SELECT toDate('2024-01-01'))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_dt; CREATE TABLE e1_dt (k DateTime) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_dt VALUES ('2024-01-01 00:00:00'), ('2024-01-01 00:00:01');
SELECT 'arm1 DateTime/DateTime', count() FROM e1_dt WHERE k IN (SELECT toDateTime('2024-01-01 00:00:00'));
SELECT 'arm1 DateTime/DateTime prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_dt WHERE k IN (SELECT toDateTime('2024-01-01 00:00:00'))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_de; CREATE TABLE e1_de (k Decimal(10, 2)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_de VALUES (1.23), (2.34);
SELECT 'arm1 Decimal/Decimal 1.23', count() FROM e1_de WHERE k IN (SELECT CAST('1.23', 'Decimal(10,2)'));
SELECT 'arm1 Decimal/Decimal prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_de WHERE k IN (SELECT CAST('1.23', 'Decimal(10,2)'))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_d64; CREATE TABLE e1_d64 (k DateTime64(3)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_d64 VALUES ('2024-01-01 00:00:00.123'), ('2024-01-01 00:00:00.124');
SELECT 'arm1 DateTime64(3)/DateTime64(3)', count() FROM e1_d64 WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)'));
SELECT 'arm1 DateTime64/DateTime64 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_d64 WHERE k IN (SELECT CAST('2024-01-01 00:00:00.123', 'DateTime64(3)'))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_uu; CREATE TABLE e1_uu (k UUID) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_uu VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), ('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT 'arm1 UUID/UUID', count() FROM e1_uu WHERE k IN (SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT 'arm1 UUID/UUID prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_uu WHERE k IN (SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_ip; CREATE TABLE e1_ip (k IPv4) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_ip VALUES ('1.2.3.4'), ('5.6.7.8');
SELECT 'arm1 IPv4/IPv4', count() FROM e1_ip WHERE k IN (SELECT toIPv4('1.2.3.4'));
SELECT 'arm1 IPv4/IPv4 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_ip WHERE k IN (SELECT toIPv4('1.2.3.4'))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_fs; CREATE TABLE e1_fs (k FixedString(3)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_fs VALUES ('ab'), ('cd');
SELECT 'arm1 FixedString/FixedString', count() FROM e1_fs WHERE k IN (SELECT toFixedString('ab', 3));
SELECT 'arm1 FixedString/FixedString prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_fs WHERE k IN (SELECT toFixedString('ab', 3))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_en; CREATE TABLE e1_en (k Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_en VALUES ('a'), ('b');
SELECT 'arm1 Enum8/Enum8', count() FROM e1_en WHERE k IN (SELECT CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'));
SELECT 'arm1 Enum8/Enum8 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_en WHERE k IN (SELECT CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS e1_bo; CREATE TABLE e1_bo (k Bool) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO e1_bo VALUES (true), (false);
SELECT 'arm1 Bool/Bool', count() FROM e1_bo WHERE k IN (SELECT CAST('true', 'Bool'));
SELECT 'arm1 Bool/Bool prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM e1_bo WHERE k IN (SELECT CAST('true', 'Bool'))) WHERE explain ILIKE '%in 1-element set%';

SELECT '--- arm 1: nested LowCardinality composites must keep pruning ---';

SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS lc_num; DROP TABLE IF EXISTS lo_num;
CREATE TABLE lc_num (a LowCardinality(UInt32), b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE lo_num (a LowCardinality(UInt32), b UInt32) ENGINE = Memory;
INSERT INTO lc_num VALUES (1, 1), (2, 2);
INSERT INTO lo_num VALUES (1, 1), (2, 2);
SELECT 'arm1 Tuple(LC(UInt32),UInt32) result',
    (SELECT count() FROM lc_num WHERE (a, b) IN (SELECT (toLowCardinality(toUInt32(1)), toUInt32(1)))) = (SELECT count() FROM lo_num WHERE (a, b) IN (SELECT (toLowCardinality(toUInt32(1)), toUInt32(1))));
SELECT 'arm1 Tuple(LC(UInt32),UInt32) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM lc_num WHERE (a, b) IN (SELECT (toLowCardinality(toUInt32(1)), toUInt32(1)))) WHERE explain ILIKE '%in 1-element set%';
SET allow_suspicious_low_cardinality_types = 0;

DROP TABLE IF EXISTS lc_str; DROP TABLE IF EXISTS lo_str;
CREATE TABLE lc_str (a LowCardinality(String), b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE lo_str (a LowCardinality(String), b UInt32) ENGINE = Memory;
INSERT INTO lc_str VALUES ('x', 1), ('y', 2);
INSERT INTO lo_str VALUES ('x', 1), ('y', 2);
SELECT 'arm1 Tuple(LC(String),UInt32) result',
    (SELECT count() FROM lc_str WHERE (a, b) IN (SELECT (toLowCardinality('x'), toUInt32(1)))) = (SELECT count() FROM lo_str WHERE (a, b) IN (SELECT (toLowCardinality('x'), toUInt32(1))));
SELECT 'arm1 Tuple(LC(String),UInt32) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM lc_str WHERE (a, b) IN (SELECT (toLowCardinality('x'), toUInt32(1)))) WHERE explain ILIKE '%in 1-element set%';

SELECT '--- arm 2: integer collapse regions stay exact (accurate cast is strict) ---';

DROP TABLE IF EXISTS cr_u8; CREATE TABLE cr_u8 (k UInt8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO cr_u8 VALUES (1), (2);
SELECT 'collapse UInt8/UInt64 257', count() FROM cr_u8 WHERE k NOT IN (SELECT toUInt64(257));
SELECT 'collapse UInt8/Int64 -1', count() FROM cr_u8 WHERE k NOT IN (SELECT toInt64(-1));
SELECT 'collapse UInt8/UInt128 1 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM cr_u8 WHERE k IN (SELECT toUInt128(1))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS cr_i64; CREATE TABLE cr_i64 (k Int64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO cr_i64 VALUES (1), (2);
SELECT 'collapse Int64/UInt64 max', count() FROM cr_i64 WHERE k NOT IN (SELECT toUInt64(18446744073709551615));
SELECT 'collapse Int64/Int32 -1', count() FROM cr_i64 WHERE k NOT IN (SELECT toInt32(-1));

DROP TABLE IF EXISTS cr_i32; CREATE TABLE cr_i32 (k Int32) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO cr_i32 VALUES (1), (2);
SELECT 'collapse Int32/Int64 -2147483649', count() FROM cr_i32 WHERE k NOT IN (SELECT toInt64(-2147483649));

DROP TABLE IF EXISTS cr_u256; CREATE TABLE cr_u256 (k UInt256) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO cr_u256 VALUES (1), (2);
SELECT 'collapse UInt256/UInt64 1', count() FROM cr_u256 WHERE k IN (SELECT toUInt64(1));
SELECT 'collapse UInt256/UInt64 1 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM cr_u256 WHERE k IN (SELECT toUInt64(1))) WHERE explain ILIKE '%in 1-element set%';

DROP TABLE IF EXISTS cr_i256; CREATE TABLE cr_i256 (k Int256) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO cr_i256 VALUES (1), (2);
SELECT 'collapse Int256/Int32 -1', count() FROM cr_i256 WHERE k NOT IN (SELECT toInt32(-1));

SELECT '--- has(): reachable cross-type, governed by the same predicate ---';

DROP TABLE IF EXISTS h_t; DROP TABLE IF EXISTS h_o;
CREATE TABLE h_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE h_o (k UInt64) ENGINE = Memory;
INSERT INTO h_t VALUES (1), (2), (3);
INSERT INTO h_o VALUES (1), (2), (3);
SELECT 'has UInt64/Int32 result',
    (SELECT count() FROM h_t WHERE has([toInt32(1)], k)) = (SELECT count() FROM h_o WHERE has([toInt32(1)], k));
SELECT 'has UInt64/Int32 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h_t WHERE has([toInt32(1)], k)) WHERE explain ILIKE '%in 1-element set%';
SELECT 'has UInt64/Int64 -1 result',
    (SELECT count() FROM h_t WHERE has([toInt64(-1)], k)) = (SELECT count() FROM h_o WHERE has([toInt64(-1)], k));
SELECT 'has UInt64/UInt8 mixed result',
    (SELECT count() FROM h_t WHERE has([toUInt8(1), toUInt8(2)], k)) = (SELECT count() FROM h_o WHERE has([toUInt8(1), toUInt8(2)], k));

SELECT '--- consumers of exactness ---';

-- extractPlainRanges fast path over numbers(): the declined atom must not corrupt the answer.
SELECT 'numbers exact range', count() FROM numbers(3) WHERE number NOT IN (SELECT '01');

DROP TABLE IF EXISTS nk_t; DROP TABLE IF EXISTS nk_o;
CREATE TABLE nk_t (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
CREATE TABLE nk_o (k Nullable(UInt64)) ENGINE = Memory;
INSERT INTO nk_t VALUES (1), (2);
INSERT INTO nk_o VALUES (1), (2);
SELECT 'Nullable key',
    (SELECT count() FROM nk_t WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM nk_o WHERE k NOT IN (SELECT '01'));

DROP TABLE IF EXISTS mm_t; DROP TABLE IF EXISTS mm_o;
CREATE TABLE mm_t (k UInt64, v UInt64, INDEX v_mm v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mm_o (k UInt64, v UInt64) ENGINE = Memory;
INSERT INTO mm_t VALUES (1, 1), (2, 2);
INSERT INTO mm_o VALUES (1, 1), (2, 2);
SELECT 'minmax on non-PK column',
    (SELECT count() FROM mm_t WHERE v NOT IN (SELECT '01')) = (SELECT count() FROM mm_o WHERE v NOT IN (SELECT '01'));

-- transform_null_in = 1 takes a different runtime cast; its behaviour must be unchanged here.
DROP TABLE IF EXISTS tn_t; DROP TABLE IF EXISTS tn_o;
CREATE TABLE tn_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE tn_o (k UInt64) ENGINE = Memory;
INSERT INTO tn_t VALUES (1), (2);
INSERT INTO tn_o VALUES (1), (2);
SELECT 'transform_null_in=1', count() FROM tn_t WHERE k IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1;
SELECT 'transform_null_in=1 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tn_t WHERE k IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%in 1-element set%';

SELECT '--- results still correct for pairs that now lose pruning ---';

DROP TABLE IF EXISTS pl_t; DROP TABLE IF EXISTS pl_o;
CREATE TABLE pl_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE pl_o (k UInt64) ENGINE = Memory;
INSERT INTO pl_t VALUES (1), (2);
INSERT INTO pl_o VALUES (1), (2);
SELECT 'loses pruning UInt64/String 1',
    (SELECT count() FROM pl_t WHERE k IN (SELECT '1')) = (SELECT count() FROM pl_o WHERE k IN (SELECT '1')),
    (SELECT count() FROM pl_t WHERE k NOT IN (SELECT '1')) = (SELECT count() FROM pl_o WHERE k NOT IN (SELECT '1'));
SELECT 'loses pruning UInt64/Float64 1.5',
    (SELECT count() FROM pl_t WHERE k IN (SELECT toFloat64(1.5))) = (SELECT count() FROM pl_o WHERE k IN (SELECT toFloat64(1.5))),
    (SELECT count() FROM pl_t WHERE k NOT IN (SELECT toFloat64(1.5))) = (SELECT count() FROM pl_o WHERE k NOT IN (SELECT toFloat64(1.5)));
SELECT 'loses pruning UInt64/DateTime',
    (SELECT count() FROM pl_t WHERE k IN (SELECT toDateTime(1))) = (SELECT count() FROM pl_o WHERE k IN (SELECT toDateTime(1)));

SELECT '--- unchanged: identical-type float atoms (separate defect, not this fix) ---';

-- These assert master's CURRENT answers. The index/runtime float equality mismatch (-0.0 vs +0.0,
-- distinct NaN payloads) is a different root cause and is deliberately untouched: identical types
-- run no conversion, so a conversion-exactness rule has nothing to say about them.
DROP TABLE IF EXISTS fz_64;
CREATE TABLE fz_64 (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO fz_64 VALUES (-0.0), (0.0);
SELECT 'float signed zero subquery', count() FROM fz_64 WHERE k NOT IN (SELECT toFloat64(0.0));
SELECT 'float signed zero literal', count() FROM fz_64 WHERE k NOT IN (0.0);

DROP TABLE IF EXISTS fz_32;
CREATE TABLE fz_32 (k Float32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO fz_32 VALUES (-0.0), (0.0);
SELECT 'float32 signed zero', count() FROM fz_32 WHERE k NOT IN (SELECT toFloat32(0.0));

DROP TABLE IF EXISTS fn_64;
CREATE TABLE fn_64 (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO fn_64 SELECT nan UNION ALL SELECT reinterpret(9221120237041090561::UInt64, 'Float64');
SELECT 'float NaN payloads', count() FROM fn_64 WHERE k NOT IN (SELECT nan);

DROP TABLE IF EXISTS ft_64;
CREATE TABLE ft_64 (a Float64, b UInt8) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO ft_64 VALUES (-0.0, 1), (0.0, 1);
SELECT 'float tuple key', count() FROM ft_64 WHERE (a, b) NOT IN (SELECT (toFloat64(0.0), toUInt8(1)));


SELECT '--- v21 H1: composite has() through the UNPACKING path ---';

DROP TABLE IF EXISTS h1; DROP TABLE IF EXISTS h1o;
CREATE TABLE h1 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE h1o (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO h1 VALUES (1, 1);
INSERT INTO h1 VALUES (2, 2);
INSERT INTO h1o VALUES (1, 1), (2, 2);
SELECT 'H1 composite has unpacked',
    (SELECT count() FROM h1 WHERE NOT has([tuple(toInt32(1), toInt32(1))], (a, b))) = (SELECT count() FROM h1o WHERE NOT has([tuple(toInt32(1), toInt32(1))], (a, b)));
-- The one-line reason: a composite is compared as ONE Field, so cross-signedness nested values are
-- unequal even though the unpacked scalars would be admitted.
SELECT 'H1 composite Field compare', has([tuple(toInt32(1), toInt32(1))], tuple(toUInt32(1), toUInt32(1)));
SELECT 'H1 scalar Field compare', has([toInt32(1)], toUInt32(1));
SELECT 'H1 cross-signedness has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1 WHERE NOT has([tuple(toInt32(1), toInt32(1))], (a, b))) WHERE explain ILIKE '%element set%';
-- boundary: identical types keep pruning
SELECT 'H1 same-type has result',
    (SELECT count() FROM h1 WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b))) = (SELECT count() FROM h1o WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b)));
SELECT 'H1 same-type has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1 WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b))) WHERE explain ILIKE '%element set%';
-- boundary: width-only pair is correct at runtime (Field collapses widths); it loses pruning here
SELECT 'H1 width-only has result',
    (SELECT count() FROM h1 WHERE NOT has([tuple(toUInt8(1), toUInt8(1))], (a, b))) = (SELECT count() FROM h1o WHERE NOT has([tuple(toUInt8(1), toUInt8(1))], (a, b)));
-- boundary: composite NOT IN over the same pair stays exact, because runtime `IN` casts the key
SELECT 'H1 composite IN result',
    (SELECT count() FROM h1 WHERE (a, b) NOT IN (SELECT (toInt32(1), toInt32(1)))) = (SELECT count() FROM h1o WHERE (a, b) NOT IN (SELECT (toInt32(1), toInt32(1))));
SELECT 'H1 composite IN prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1 WHERE (a, b) NOT IN (SELECT (toInt32(1), toInt32(1)))) WHERE explain ILIKE '%element set%';
-- boundary: SCALAR has() is unaffected and must keep pruning
DROP TABLE IF EXISTS h1s; DROP TABLE IF EXISTS h1so;
CREATE TABLE h1s (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE h1so (k UInt64) ENGINE = Memory;
INSERT INTO h1s VALUES (1), (2), (3);
INSERT INTO h1so VALUES (1), (2), (3);
SELECT 'H1 scalar has result',
    (SELECT count() FROM h1s WHERE has([toInt32(1)], k)) = (SELECT count() FROM h1so WHERE has([toInt32(1)], k)),
    (SELECT count() FROM h1s WHERE NOT has([toInt32(1)], k)) = (SELECT count() FROM h1so WHERE NOT has([toInt32(1)], k));
SELECT 'H1 scalar has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1s WHERE has([toInt32(1)], k)) WHERE explain ILIKE '%element set%';
SELECT 'H1 scalar NOT has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1s WHERE NOT has([toInt32(1)], k)) WHERE explain ILIKE '%element set%';
-- boundary: a composite KEY EXPRESSION under a SCALAR has() is not a composite comparison at all
DROP TABLE IF EXISTS h1x;
CREATE TABLE h1x (p String) ENGINE = MergeTree ORDER BY reverse(tuple(reverse(p), hex(p))) SETTINGS index_granularity = 1;
INSERT INTO h1x VALUES ('abc'), ('xyz');
SELECT 'H1 composite key expr scalar has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1x WHERE has(['abc'], p) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain ILIKE '%element set%';

SELECT '--- v18 B1: a custom name over an integer must not skip the conversion-target check ---';

DROP TABLE IF EXISTS b1; DROP TABLE IF EXISTS b1o;
CREATE TABLE b1 (k UInt8) ENGINE = MergeTree ORDER BY toString(k) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE b1o (k UInt8) ENGINE = Memory;
INSERT INTO b1 VALUES (0);
INSERT INTO b1 VALUES (1);
INSERT INTO b1o VALUES (0), (1);
SELECT 'B1 Bool element over toString key',
    (SELECT count() FROM b1 WHERE k IN (SELECT true)) = (SELECT count() FROM b1o WHERE k IN (SELECT true));
-- the DAG output really differs, which is why a conversion runs and has to be checked
SELECT 'B1 toString(Bool) differs', toString(true) != toString(toUInt8(1));
-- localisation: without a key transform the same element is already correct
DROP TABLE IF EXISTS b1n; DROP TABLE IF EXISTS b1no;
CREATE TABLE b1n (k UInt8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE b1no (k UInt8) ENGINE = Memory;
INSERT INTO b1n VALUES (0);
INSERT INTO b1n VALUES (1);
INSERT INTO b1no VALUES (0), (1);
SELECT 'B1 no key transform',
    (SELECT count() FROM b1n WHERE k IN (SELECT true)) = (SELECT count() FROM b1no WHERE k IN (SELECT true));
-- and the plain-integer twin on the SAME table keeps its atom, so this is not a blanket decline
SELECT 'B1 UInt8 twin prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM b1 WHERE k IN (SELECT toUInt8(1))) WHERE explain ILIKE '%element set%';
SELECT 'B1 UInt8 twin result',
    (SELECT count() FROM b1 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM b1o WHERE k IN (SELECT toUInt8(1)));
-- the scalar equals/notEquals path is a different atom kind and must not change
SELECT 'B1 scalar notEquals unchanged', count() FROM b1 WHERE k != true;

SELECT '--- v17 Z1/Z1b: a fast-path CAST that is injective on the key but collapses the element ---';

DROP TABLE IF EXISTS z1; DROP TABLE IF EXISTS z1o;
CREATE TABLE z1 (k UInt32) ENGINE = MergeTree ORDER BY (k::UInt64) PARTITION BY (k::UInt64);
CREATE TABLE z1o (k UInt32) ENGINE = Memory;
INSERT INTO z1 VALUES (1), (2);
INSERT INTO z1o VALUES (1), (2);
SELECT 'Z1 UInt32 key cast to UInt64',
    (SELECT count() FROM z1 WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM z1o WHERE k NOT IN (SELECT '01'));
DROP TABLE IF EXISTS z1b; DROP TABLE IF EXISTS z1bo;
CREATE TABLE z1b (k UInt64) ENGINE = MergeTree ORDER BY (k::String) PARTITION BY (k::String);
CREATE TABLE z1bo (k UInt64) ENGINE = Memory;
INSERT INTO z1b VALUES (1), (2);
INSERT INTO z1bo VALUES (1), (2);
SELECT 'Z1b UInt64 key cast to String',
    (SELECT count() FROM z1b WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM z1bo WHERE k NOT IN (SELECT '01'));

SELECT '--- v17 Z2: a non-injective key transform still over-prunes the POSITIVE direction ---';

-- `relaxed` only forces can_be_false, never widens can_be_true, so a relaxed atom does not protect
-- `IN`. The two rows must be in separate granules and `length` must SEPARATE the round-trip pair.
DROP TABLE IF EXISTS z2; DROP TABLE IF EXISTS z2o;
CREATE TABLE z2 (s String) ENGINE = MergeTree ORDER BY length(s) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE z2o (s String) ENGINE = Memory;
INSERT INTO z2 VALUES ('1');
INSERT INTO z2 VALUES ('01');
INSERT INTO z2o VALUES ('1'), ('01');
SELECT 'Z2 non-injective key, positive IN',
    (SELECT count() FROM z2 WHERE s IN (SELECT toUInt8(1))) = (SELECT count() FROM z2o WHERE s IN (SELECT toUInt8(1)));
SELECT 'Z2 negative direction control',
    (SELECT count() FROM z2 WHERE s NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM z2o WHERE s NOT IN (SELECT toUInt8(1)));
-- Z3 scope boundary: a transform that COLLAPSES the pair the same way the element cast does is
-- not a carrier in either direction. This is why 03762's moved block is correctness-neutral.
DROP TABLE IF EXISTS z3; DROP TABLE IF EXISTS z3o;
CREATE TABLE z3 (s String) ENGINE = MergeTree ORDER BY (s::UInt64) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE z3o (s String) ENGINE = Memory;
INSERT INTO z3 VALUES ('1');
INSERT INTO z3 VALUES ('01');
INSERT INTO z3o VALUES ('1'), ('01');
SELECT 'Z3 collapsing transform both directions',
    (SELECT count() FROM z3 WHERE s IN (SELECT toUInt8(1))) = (SELECT count() FROM z3o WHERE s IN (SELECT toUInt8(1))),
    (SELECT count() FROM z3 WHERE s NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM z3o WHERE s NOT IN (SELECT toUInt8(1)));

SELECT '--- v15: the set-transforming DAG carrier, and the fast-path spelling that is not one ---';

DROP TABLE IF EXISTS dg; DROP TABLE IF EXISTS dgo;
CREATE TABLE dg (k UInt64) ENGINE = MergeTree ORDER BY toString(k) PARTITION BY toString(k);
CREATE TABLE dgo (k UInt64) ENGINE = Memory;
INSERT INTO dg VALUES (1), (2);
INSERT INTO dgo VALUES (1), (2);
SELECT 'DAG carrier toString(k)',
    (SELECT count() FROM dg WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM dgo WHERE k NOT IN (SELECT '01'));
-- a bare CAST takes the fast path, which converts to the CAST result type instead, so no collapse
-- happens and the atom must be KEPT
DROP TABLE IF EXISTS dgc;
CREATE TABLE dgc (k UInt64) ENGINE = MergeTree ORDER BY (k::String) PARTITION BY (k::String);
INSERT INTO dgc VALUES (1), (2);
SELECT 'DAG ::String twin prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM dgc WHERE k IN (SELECT 'x')) WHERE explain ILIKE '%element set%';

SELECT '--- v14 has()/composite carriers: the same predicate fixes has() too ---';

DROP TABLE IF EXISTS c_g_has; DROP TABLE IF EXISTS o_g_has;
CREATE TABLE c_g_has (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_g_has (k UInt64) ENGINE = Memory;
INSERT INTO c_g_has VALUES (1), (2);
INSERT INTO o_g_has VALUES (1), (2);
SELECT 'G-has UInt64/Decimal64(1)',
    (SELECT count() FROM c_g_has WHERE NOT has([CAST(1.5, 'Decimal64(1)')], k)) = (SELECT count() FROM o_g_has WHERE NOT has([CAST(1.5, 'Decimal64(1)')], k));

DROP TABLE IF EXISTS c_v_has; DROP TABLE IF EXISTS o_v_has;
CREATE TABLE c_v_has (k Int64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_v_has (k Int64) ENGINE = Memory;
INSERT INTO c_v_has VALUES (1), (2);
INSERT INTO o_v_has VALUES (1), (2);
SELECT 'V-has Int64/Decimal(10,2)',
    (SELECT count() FROM c_v_has WHERE NOT has([CAST('1.50', 'Decimal(10,2)')], k)) = (SELECT count() FROM o_v_has WHERE NOT has([CAST('1.50', 'Decimal(10,2)')], k));

DROP TABLE IF EXISTS c_cv; DROP TABLE IF EXISTS o_cv;
CREATE TABLE c_cv (a Int64, b UInt8) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE o_cv (a Int64, b UInt8) ENGINE = Memory;
INSERT INTO c_cv VALUES (1, 1), (2, 1);
INSERT INTO o_cv VALUES (1, 1), (2, 1);
SELECT 'CV-has unpacked (Int64,UInt8)/(Decimal(10,2),UInt8)',
    (SELECT count() FROM c_cv WHERE NOT has([(CAST('1.50', 'Decimal(10,2)'), toUInt8(1))], (a, b))) = (SELECT count() FROM o_cv WHERE NOT has([(CAST('1.50', 'Decimal(10,2)'), toUInt8(1))], (a, b)));
SELECT 'CV-in unpacked',
    (SELECT count() FROM c_cv WHERE (a, b) NOT IN (SELECT (CAST('1.50', 'Decimal(10,2)'), toUInt8(1)))) = (SELECT count() FROM o_cv WHERE (a, b) NOT IN (SELECT (CAST('1.50', 'Decimal(10,2)'), toUInt8(1))));

DROP TABLE IF EXISTS c_cvp; DROP TABLE IF EXISTS o_cvp;
CREATE TABLE c_cvp (kt Tuple(Int64, UInt8)) ENGINE = MergeTree ORDER BY kt PARTITION BY kt;
CREATE TABLE o_cvp (kt Tuple(Int64, UInt8)) ENGINE = Memory;
INSERT INTO c_cvp VALUES ((1, 1)), ((2, 1));
INSERT INTO o_cvp VALUES ((1, 1)), ((2, 1));
SELECT 'CV-has packed Tuple(Int64,UInt8)',
    (SELECT count() FROM c_cvp WHERE NOT has([(CAST('1.50', 'Decimal(10,2)'), toUInt8(1))], kt)) = (SELECT count() FROM o_cvp WHERE NOT has([(CAST('1.50', 'Decimal(10,2)'), toUInt8(1))], kt));

DROP TABLE IF EXISTS c_cg; DROP TABLE IF EXISTS o_cg;
CREATE TABLE c_cg (a UInt64, b UInt8) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE o_cg (a UInt64, b UInt8) ENGINE = Memory;
INSERT INTO c_cg VALUES (1, 1), (2, 1);
INSERT INTO o_cg VALUES (1, 1), (2, 1);
SELECT 'CG-has (UInt64,UInt8)/(Decimal64(1),UInt8)',
    (SELECT count() FROM c_cg WHERE NOT has([(CAST(1.5, 'Decimal64(1)'), toUInt8(1))], (a, b))) = (SELECT count() FROM o_cg WHERE NOT has([(CAST(1.5, 'Decimal64(1)'), toUInt8(1))], (a, b)));

DROP TABLE IF EXISTS c_cn6; DROP TABLE IF EXISTS o_cn6;
CREATE TABLE c_cn6 (a Decimal(20, 4), b UInt8) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE o_cn6 (a Decimal(20, 4), b UInt8) ENGINE = Memory;
INSERT INTO c_cn6 VALUES (1.0001, 1), (2.0000, 1);
INSERT INTO o_cn6 VALUES (1.0001, 1), (2.0000, 1);
-- the truncating Decimal pair under-approximates, so it is the POSITIVE direction that over-prunes
SELECT 'CN6-in (Decimal(20,4),UInt8)/(Decimal(10,2),UInt8)',
    (SELECT count() FROM c_cn6 WHERE (a, b) IN (SELECT (CAST('1.00', 'Decimal(10,2)'), toUInt8(1)))) = (SELECT count() FROM o_cn6 WHERE (a, b) IN (SELECT (CAST('1.00', 'Decimal(10,2)'), toUInt8(1))));
SELECT 'N6-in scalar Decimal(20,4)/Decimal(10,2)',
    (SELECT count() FROM c_cn6 WHERE a IN (SELECT CAST('1.00', 'Decimal(10,2)'))) = (SELECT count() FROM o_cn6 WHERE a IN (SELECT CAST('1.00', 'Decimal(10,2)')));

SELECT '--- composite cross-type: pruning is withdrawn for a PACKED composite key (the 03733 shapes) ---';

DROP TABLE IF EXISTS t33; DROP TABLE IF EXISTS t33o;
CREATE TABLE t33 (kt Tuple(UInt32, UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE t33o (kt Tuple(UInt32, UInt32)) ENGINE = Memory;
INSERT INTO t33 VALUES ((10, 0));
INSERT INTO t33 VALUES ((50000, 0));
INSERT INTO t33 VALUES ((7, 7));
INSERT INTO t33o VALUES ((10, 0)), ((50000, 0)), ((7, 7));
-- The literal's element type is `Tuple(UInt16, UInt8)` against a `Tuple(UInt32, UInt32)` key, so the
-- two differ only in the width of native integers and the runtime `has` compares them identically.
-- Pruning is nevertheless withdrawn, and by the OTHER rule: with a PACKED composite key column the
-- unpack loop in `tryPrepareSetColumnsForIndex` does not run, so the per-column check receives the
-- whole `Tuple`/`Array` pair, and it is exact only for an `equals`-equal or plain-integer pair - a
-- composite runtime cast can throw where the preparation cast merely returns NULL. The result cell
-- above is what keeps this honest: the answer still matches the oracle, only the optimization is lost.
SELECT 'T33 packed tuple has result',
    (SELECT count() FROM t33 WHERE has([(10, 0), (50000, 0)], kt)) = (SELECT count() FROM t33o WHERE has([(10, 0), (50000, 0)], kt));
SELECT 'T33 packed tuple has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t33 WHERE has([(10, 0), (50000, 0)], kt)) WHERE explain ILIKE '%element set%';
SELECT 'T33 packed tuple IN result',
    (SELECT count() FROM t33 WHERE kt IN (SELECT (toUInt16(10), toUInt8(0)))) = (SELECT count() FROM t33o WHERE kt IN (SELECT (toUInt16(10), toUInt8(0))));

DROP TABLE IF EXISTS a33; DROP TABLE IF EXISTS a33o;
CREATE TABLE a33 (ak Array(UInt32)) ENGINE = MergeTree ORDER BY ak SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE a33o (ak Array(UInt32)) ENGINE = Memory;
INSERT INTO a33 VALUES ([10, 11]);
INSERT INTO a33 VALUES ([50000, 50001]);
INSERT INTO a33 VALUES ([7, 7]);
INSERT INTO a33o VALUES ([10, 11]), ([50000, 50001]), ([7, 7]);
-- Same width-only shape one container deeper (`Array(Array(UInt16))` literal against an `Array(UInt32)`
-- key), and the same packed-key rule applies, so pruning is withdrawn here too.
SELECT 'A33 array key has result',
    (SELECT count() FROM a33 WHERE has([[10, 11], [50000, 50001]], ak)) = (SELECT count() FROM a33o WHERE has([[10, 11], [50000, 50001]], ak));
SELECT 'A33 array key has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM a33 WHERE has([[10, 11], [50000, 50001]], ak)) WHERE explain ILIKE '%element set%';

-- Native widths collapse in a Field, signedness does not, and the 128/256-bit tags do not either:
-- this is exactly the boundary the composite identity rule has to draw.
SELECT 'Field width u8 vs u64', has([tuple(toUInt8(1))], tuple(toUInt64(1)));
SELECT 'Field width i8 vs i64', has([tuple(toInt8(1))], tuple(toInt64(1)));
SELECT 'Field signedness i32 vs u32', has([tuple(toInt32(1))], tuple(toUInt32(1)));
SELECT 'Field width u64 vs u128', has([tuple(toUInt64(1))], tuple(toUInt128(1)));
SELECT 'Field width u128 vs u256', has([tuple(toUInt128(1))], tuple(toUInt256(1)));
-- A scalar element takes the other rule (the preparation cast, which nulls instead of truncating), so
-- native-vs-128-bit DOES match there. The two rows below are the asymmetry that forbids unifying them.
SELECT 'Field scalar u64 vs u128', has([toUInt64(1)], toUInt128(1));

SELECT '--- composite has() over TWO key columns: the one shape where the composite rule decides ---';

-- With a two-column key the per-column checks see the UNPACKED scalars and admit any integer pair, so
-- the composite rule is the deciding gate here and these cells are what pin it. (With a PACKED tuple or
-- array key the per-column check sees the whole composite instead and rejects a width-only pair before
-- this rule is consulted - see the residual noted in the PR description.)
DROP TABLE IF EXISTS w2c; DROP TABLE IF EXISTS o2c;
CREATE TABLE w2c (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE o2c (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO w2c VALUES (10, 0); INSERT INTO w2c VALUES (50000, 0); INSERT INTO w2c VALUES (7, 7);
INSERT INTO o2c VALUES (10, 0), (50000, 0), (7, 7);

-- Width-only: the literal is `Array(Tuple(UInt16, UInt8))` against a `(UInt32, UInt32)` key, which the
-- runtime compares identically, so the atom must KEEP pruning.
SELECT 'W2C width-only has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w2c WHERE has([(10, 0), (50000, 0)], (a, b))) WHERE explain ILIKE '%element set%';
SELECT 'W2C width-only has',
    (SELECT count() FROM w2c WHERE has([(10, 0), (50000, 0)], (a, b))) = (SELECT count() FROM o2c WHERE has([(10, 0), (50000, 0)], (a, b)));

-- 128-bit: a distinct `Field` variant, so the runtime never matches the pair and the atom must decline.
SELECT 'W2C 128-bit has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w2c WHERE has([tuple(toUInt128(10), toUInt128(0)), tuple(toUInt128(50000), toUInt128(0))], (a, b))) WHERE explain ILIKE '%element set%';
SELECT 'W2C 128-bit has',
    (SELECT count() FROM w2c WHERE has([tuple(toUInt128(10), toUInt128(0)), tuple(toUInt128(50000), toUInt128(0))], (a, b))) = (SELECT count() FROM o2c WHERE has([tuple(toUInt128(10), toUInt128(0)), tuple(toUInt128(50000), toUInt128(0))], (a, b)));
DROP TABLE w2c; DROP TABLE o2c;

-- Signedness, the other direction of the same rule: an `Int32` key against an unsigned literal.
DROP TABLE IF EXISTS s2c; DROP TABLE IF EXISTS p2c;
CREATE TABLE s2c (a Int32, b Int32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE p2c (a Int32, b Int32) ENGINE = Memory;
INSERT INTO s2c VALUES (1, 1); INSERT INTO s2c VALUES (2, 2); INSERT INTO s2c VALUES (3, 3);
INSERT INTO p2c VALUES (1, 1), (2, 2), (3, 3);
SELECT 'S2C signedness has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM s2c WHERE has([tuple(toUInt16(1), toUInt16(1)), tuple(toUInt16(2), toUInt16(2))], (a, b))) WHERE explain ILIKE '%element set%';
SELECT 'S2C signedness has',
    (SELECT count() FROM s2c WHERE has([tuple(toUInt16(1), toUInt16(1)), tuple(toUInt16(2), toUInt16(2))], (a, b))) = (SELECT count() FROM p2c WHERE has([tuple(toUInt16(1), toUInt16(1)), tuple(toUInt16(2), toUInt16(2))], (a, b)));
DROP TABLE s2c; DROP TABLE p2c;

SELECT '--- composite has() over a TRANSFORMING key expression: the left type is not reconstructible ---';

-- `data_types` always carries the type of the KEY column, so under a transforming key expression it is
-- the type of the transformed key, not of the runtime left tuple. Deciding composite identity from it
-- compares the wrong pair, and because `negate` is injective the atom stays EXACT, so `NOT has` prunes
-- a partition that still holds a match. The pair below is `(UInt32, UInt32)` against
-- `Tuple(Int64, Int64)`: the runtime `Field` compare is 0 (different signedness), so `NOT has` is true
-- for every row, yet master's reconstruction saw the negated key's type and admitted the pair.
DROP TABLE IF EXISTS ctn; DROP TABLE IF EXISTS ctno;
CREATE TABLE ctn (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (negate(a), negate(b)) PARTITION BY (negate(a), negate(b)) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE ctno (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO ctn VALUES (1, 1); INSERT INTO ctn VALUES (2, 2);
INSERT INTO ctno VALUES (1, 1), (2, 2);
SELECT 'CTN transforming key NOT has',
    (SELECT count() FROM ctn WHERE NOT has([tuple(toInt64(1), toInt64(1))], (a, b))) = (SELECT count() FROM ctno WHERE NOT has([tuple(toInt64(1), toInt64(1))], (a, b)));
SELECT 'CTN transforming key NOT has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ctn WHERE NOT has([tuple(toInt64(1), toInt64(1))], (a, b))) WHERE explain ILIKE '%element set%';
DROP TABLE ctn; DROP TABLE ctno;

-- The must-not-regress partner: a same-type literal over a NON-transforming key, otherwise identical.
-- Declining every composite `has` would pass the two cells above and fail this one.
DROP TABLE IF EXISTS ctp; DROP TABLE IF EXISTS ctpo;
CREATE TABLE ctp (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE ctpo (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO ctp VALUES (1, 1); INSERT INTO ctp VALUES (2, 2);
INSERT INTO ctpo VALUES (1, 1), (2, 2);
-- Assert the partition reduction, not just the atom's presence: a relaxed atom is still installed
-- (still prints `element set`) but sets `can_be_false = true` before the negation, so `NOT has` stops
-- pruning while the answer stays correct. Only the part count separates exact from relaxed here.
-- `Parts:` is format-independent, so the `explain_query_plan_default` pin at the top suffices.
SELECT 'CTP plain key NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ctp WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b))) WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'CTP plain key NOT has',
    (SELECT count() FROM ctp WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b))) = (SELECT count() FROM ctpo WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b)));
DROP TABLE ctp; DROP TABLE ctpo;

SELECT '--- composite has(): the attribute axis, pinned per direction ---';

-- Every other attribute-axis control in this file is a scalar `IN`. Without these three the composite
-- identity rule could be reverted to comparing canonical names and the whole file would still pass,
-- silently losing composite pruning again - which is the regression the first cell below catches.

-- A time zone is an attribute `equals` treats as interchangeable and a `Field` does not represent at
-- all, so it cannot change the runtime verdict: this pair must KEEP pruning in both directions.
DROP TABLE IF EXISTS ca_tz; DROP TABLE IF EXISTS oa_tz;
CREATE TABLE ca_tz (kt Tuple(DateTime('UTC'))) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE oa_tz (kt Tuple(DateTime('UTC'))) ENGINE = Memory;
-- `INSERT ... VALUES ((toDateTime(100)))` is rejected with `Code: 53` for a 1-tuple column, so build
-- the rows with `SELECT tuple(...)`.
INSERT INTO ca_tz SELECT tuple(toDateTime(100 + number * 100)) FROM numbers(3);
INSERT INTO oa_tz SELECT tuple(toDateTime(100 + number * 100)) FROM numbers(3);
SELECT 'attr Tuple(DateTime(UTC))/Tuple(DateTime) has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ca_tz WHERE has([tuple(CAST(100, 'DateTime'))], kt)) WHERE explain ILIKE '%element set%';
SELECT 'attr Tuple(DateTime(UTC))/Tuple(DateTime) has',
    (SELECT count() FROM ca_tz WHERE has([tuple(CAST(100, 'DateTime'))], kt)) = (SELECT count() FROM oa_tz WHERE has([tuple(CAST(100, 'DateTime'))], kt));
SELECT 'attr Tuple(DateTime(UTC))/Tuple(DateTime) NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ca_tz WHERE NOT has([tuple(CAST(100, 'DateTime'))], kt)) WHERE explain ILIKE '%element set%';
SELECT 'attr Tuple(DateTime(UTC))/Tuple(DateTime) NOT has',
    (SELECT count() FROM ca_tz WHERE NOT has([tuple(CAST(100, 'DateTime'))], kt)) = (SELECT count() FROM oa_tz WHERE NOT has([tuple(CAST(100, 'DateTime'))], kt));
DROP TABLE ca_tz; DROP TABLE oa_tz;

-- The other direction of the same axis: a custom name IS load-bearing, because `Bool`'s cast wrapper
-- clamps every nonzero value to 1, so the preparation direction is not injective even though the
-- runtime matches the pair. Master admits it; the atom must now be absent.
DROP TABLE IF EXISTS ca_bl; DROP TABLE IF EXISTS oa_bl;
CREATE TABLE ca_bl (kt Tuple(Bool)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE oa_bl (kt Tuple(Bool)) ENGINE = Memory;
INSERT INTO ca_bl SELECT tuple(number % 2 = 1) FROM numbers(3);
INSERT INTO oa_bl SELECT tuple(number % 2 = 1) FROM numbers(3);
SELECT 'attr Tuple(Bool)/Tuple(UInt8) has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ca_bl WHERE has([tuple(toUInt8(1))], kt)) WHERE explain ILIKE '%element set%';
SELECT 'attr Tuple(Bool)/Tuple(UInt8) has',
    (SELECT count() FROM ca_bl WHERE has([tuple(toUInt8(1))], kt)) = (SELECT count() FROM oa_bl WHERE has([tuple(toUInt8(1))], kt));
DROP TABLE ca_bl; DROP TABLE oa_bl;

-- A native integer against a 128-bit one keeps its own `Field` variant, so the runtime never matches
-- the pair (`has([tuple(toUInt128(1), toUInt128(0))], tuple(toUInt64(1), toUInt64(0)))` is 0) even
-- though the preparation cast would be lossless. Master admits it; the atom must now be absent, and
-- declining agrees with the oracle.
DROP TABLE IF EXISTS ca_w; DROP TABLE IF EXISTS oa_w;
CREATE TABLE ca_w (kt Tuple(UInt64, UInt64)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE oa_w (kt Tuple(UInt64, UInt64)) ENGINE = Memory;
INSERT INTO ca_w SELECT tuple(toUInt64(number), toUInt64(0)) FROM numbers(3);
INSERT INTO oa_w SELECT tuple(toUInt64(number), toUInt64(0)) FROM numbers(3);
SELECT 'width Tuple(UInt64,UInt64)/Tuple(UInt128,UInt128) has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ca_w WHERE has([tuple(toUInt128(1), toUInt128(0))], kt)) WHERE explain ILIKE '%element set%';
SELECT 'width Tuple(UInt64,UInt64)/Tuple(UInt128,UInt128) has',
    (SELECT count() FROM ca_w WHERE has([tuple(toUInt128(1), toUInt128(0))], kt)) = (SELECT count() FROM oa_w WHERE has([tuple(toUInt128(1), toUInt128(0))], kt));
DROP TABLE ca_w; DROP TABLE oa_w;

SELECT '--- named tuples: the cast maps fields by name, so the pair must decline ---';

DROP TABLE IF EXISTS nt; DROP TABLE IF EXISTS nto;
CREATE TABLE nt (kt Tuple(a UInt8, b UInt8)) ENGINE = MergeTree ORDER BY kt PARTITION BY kt;
CREATE TABLE nto (kt Tuple(a UInt8, b UInt8)) ENGINE = Memory;
INSERT INTO nt VALUES ((1, 1));
INSERT INTO nt VALUES ((2, 2));
INSERT INTO nto VALUES ((1, 1)), ((2, 2));
SELECT 'named tuple result',
    (SELECT count() FROM nt WHERE kt NOT IN (SELECT CAST((1, 1), 'Tuple(c UInt8, d UInt8)'))) = (SELECT count() FROM nto WHERE kt NOT IN (SELECT CAST((1, 1), 'Tuple(c UInt8, d UInt8)')));
SELECT 'named tuple declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM nt WHERE kt NOT IN (SELECT CAST((1, 1), 'Tuple(c UInt8, d UInt8)'))) WHERE explain ILIKE '%element set%';

SELECT '--- composite IN over a narrowing element: pruning is withdrawn, matching the oracle ---';

-- Pre-existing behaviour recorded for completeness: on a narrowing composite pair the runtime cast
-- throws CANNOT_CONVERT_TYPE while master silently pruned instead. Declining the atom makes
-- MergeTree agree with the ENGINE = Memory oracle, which also throws.
DROP TABLE IF EXISTS d1;
CREATE TABLE d1 (kt Tuple(UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO d1 VALUES (tuple(257));
INSERT INTO d1 VALUES (tuple(1));
SELECT count() FROM d1 WHERE kt IN (SELECT tuple(toUInt8(1))); -- { serverError CANNOT_CONVERT_TYPE }
-- boundary: the WIDENING direction is unaffected and keeps its result
DROP TABLE IF EXISTS w1; DROP TABLE IF EXISTS w1o;
CREATE TABLE w1 (kt Tuple(UInt8, UInt8)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE w1o (kt Tuple(UInt8, UInt8)) ENGINE = Memory;
INSERT INTO w1 VALUES ((1, 1));
INSERT INTO w1 VALUES ((2, 2));
INSERT INTO w1o VALUES ((1, 1)), ((2, 2));
SELECT 'D1 widening direction',
    (SELECT count() FROM w1 WHERE kt IN (SELECT (toUInt32(1), toUInt32(1)))) = (SELECT count() FROM w1o WHERE kt IN (SELECT (toUInt32(1), toUInt32(1))));
-- boundary: the SCALAR narrowing case is unaffected at default settings; arm 2 stays intact
DROP TABLE IF EXISTS s1; DROP TABLE IF EXISTS s1o;
CREATE TABLE s1 (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE s1o (k UInt32) ENGINE = Memory;
INSERT INTO s1 VALUES (257);
INSERT INTO s1 VALUES (1);
INSERT INTO s1o VALUES (257), (1);
SELECT 'D1 scalar narrowing at default settings',
    (SELECT count() FROM s1 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM s1o WHERE k IN (SELECT toUInt8(1)));

SELECT '--- integer composites: pruning is withdrawn for both callers, results stay correct ---';

-- 8x8 packed integer composites, plain and Nullable. Generated; do not thin.
DROP TABLE IF EXISTS c_gc_uint8; DROP TABLE IF EXISTS o_gc_uint8;
CREATE TABLE c_gc_uint8 (kt Tuple(UInt8, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_uint8 (kt Tuple(UInt8, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_uint8 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_uint8 VALUES ((1, 1)), ((2, 1));
SELECT 'grid P UInt8/UInt8',
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid P UInt8/UInt16',
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid P UInt8/UInt32',
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid P UInt8/UInt64',
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid P UInt8/Int8',
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid P UInt8/Int16',
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid P UInt8/Int32',
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid P UInt8/Int64',
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gc_uint8; DROP TABLE o_gc_uint8;
DROP TABLE IF EXISTS c_gc_uint16; DROP TABLE IF EXISTS o_gc_uint16;
CREATE TABLE c_gc_uint16 (kt Tuple(UInt16, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_uint16 (kt Tuple(UInt16, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_uint16 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_uint16 VALUES ((1, 1)), ((2, 1));
SELECT 'grid P UInt16/UInt8',
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid P UInt16/UInt16',
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid P UInt16/UInt32',
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid P UInt16/UInt64',
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid P UInt16/Int8',
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid P UInt16/Int16',
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid P UInt16/Int32',
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid P UInt16/Int64',
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gc_uint16; DROP TABLE o_gc_uint16;
DROP TABLE IF EXISTS c_gc_uint32; DROP TABLE IF EXISTS o_gc_uint32;
CREATE TABLE c_gc_uint32 (kt Tuple(UInt32, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_uint32 (kt Tuple(UInt32, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_uint32 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_uint32 VALUES ((1, 1)), ((2, 1));
SELECT 'grid P UInt32/UInt8',
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid P UInt32/UInt16',
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid P UInt32/UInt32',
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid P UInt32/UInt64',
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid P UInt32/Int8',
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid P UInt32/Int16',
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid P UInt32/Int32',
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid P UInt32/Int64',
    (SELECT count() FROM c_gc_uint32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gc_uint32; DROP TABLE o_gc_uint32;
DROP TABLE IF EXISTS c_gc_uint64; DROP TABLE IF EXISTS o_gc_uint64;
CREATE TABLE c_gc_uint64 (kt Tuple(UInt64, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_uint64 (kt Tuple(UInt64, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_uint64 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_uint64 VALUES ((1, 1)), ((2, 1));
SELECT 'grid P UInt64/UInt8',
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid P UInt64/UInt16',
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid P UInt64/UInt32',
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid P UInt64/UInt64',
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid P UInt64/Int8',
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid P UInt64/Int16',
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid P UInt64/Int32',
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid P UInt64/Int64',
    (SELECT count() FROM c_gc_uint64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_uint64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gc_uint64; DROP TABLE o_gc_uint64;
DROP TABLE IF EXISTS c_gc_int8; DROP TABLE IF EXISTS o_gc_int8;
CREATE TABLE c_gc_int8 (kt Tuple(Int8, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_int8 (kt Tuple(Int8, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_int8 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_int8 VALUES ((1, 1)), ((2, 1));
SELECT 'grid P Int8/UInt8',
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid P Int8/UInt16',
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid P Int8/UInt32',
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid P Int8/UInt64',
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid P Int8/Int8',
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid P Int8/Int16',
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid P Int8/Int32',
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid P Int8/Int64',
    (SELECT count() FROM c_gc_int8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gc_int8; DROP TABLE o_gc_int8;
DROP TABLE IF EXISTS c_gc_int16; DROP TABLE IF EXISTS o_gc_int16;
CREATE TABLE c_gc_int16 (kt Tuple(Int16, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_int16 (kt Tuple(Int16, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_int16 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_int16 VALUES ((1, 1)), ((2, 1));
SELECT 'grid P Int16/UInt8',
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid P Int16/UInt16',
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid P Int16/UInt32',
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid P Int16/UInt64',
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid P Int16/Int8',
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid P Int16/Int16',
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid P Int16/Int32',
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid P Int16/Int64',
    (SELECT count() FROM c_gc_int16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gc_int16; DROP TABLE o_gc_int16;
DROP TABLE IF EXISTS c_gc_int32; DROP TABLE IF EXISTS o_gc_int32;
CREATE TABLE c_gc_int32 (kt Tuple(Int32, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_int32 (kt Tuple(Int32, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_int32 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_int32 VALUES ((1, 1)), ((2, 1));
SELECT 'grid P Int32/UInt8',
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid P Int32/UInt16',
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid P Int32/UInt32',
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid P Int32/UInt64',
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid P Int32/Int8',
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid P Int32/Int16',
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid P Int32/Int32',
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid P Int32/Int64',
    (SELECT count() FROM c_gc_int32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gc_int32; DROP TABLE o_gc_int32;
DROP TABLE IF EXISTS c_gc_int64; DROP TABLE IF EXISTS o_gc_int64;
CREATE TABLE c_gc_int64 (kt Tuple(Int64, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_int64 (kt Tuple(Int64, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_int64 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_int64 VALUES ((1, 1)), ((2, 1));
SELECT 'grid P Int64/UInt8',
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid P Int64/UInt16',
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid P Int64/UInt32',
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid P Int64/UInt64',
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid P Int64/Int8',
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid P Int64/Int16',
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid P Int64/Int32',
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid P Int64/Int64',
    (SELECT count() FROM c_gc_int64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gc_int64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_int64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gc_int64; DROP TABLE o_gc_int64;
DROP TABLE IF EXISTS c_gcn_uint8; DROP TABLE IF EXISTS o_gcn_uint8;
CREATE TABLE c_gcn_uint8 (kt Tuple(Nullable(UInt8), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_uint8 (kt Tuple(Nullable(UInt8), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_uint8 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_uint8 VALUES ((1, 1)), ((2, 1));
SELECT 'grid N UInt8/UInt8',
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid N UInt8/UInt16',
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid N UInt8/UInt32',
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid N UInt8/UInt64',
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid N UInt8/Int8',
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid N UInt8/Int16',
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid N UInt8/Int32',
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid N UInt8/Int64',
    (SELECT count() FROM c_gcn_uint8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gcn_uint8; DROP TABLE o_gcn_uint8;
DROP TABLE IF EXISTS c_gcn_uint16; DROP TABLE IF EXISTS o_gcn_uint16;
CREATE TABLE c_gcn_uint16 (kt Tuple(Nullable(UInt16), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_uint16 (kt Tuple(Nullable(UInt16), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_uint16 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_uint16 VALUES ((1, 1)), ((2, 1));
SELECT 'grid N UInt16/UInt8',
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid N UInt16/UInt16',
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid N UInt16/UInt32',
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid N UInt16/UInt64',
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid N UInt16/Int8',
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid N UInt16/Int16',
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid N UInt16/Int32',
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid N UInt16/Int64',
    (SELECT count() FROM c_gcn_uint16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gcn_uint16; DROP TABLE o_gcn_uint16;
DROP TABLE IF EXISTS c_gcn_uint32; DROP TABLE IF EXISTS o_gcn_uint32;
CREATE TABLE c_gcn_uint32 (kt Tuple(Nullable(UInt32), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_uint32 (kt Tuple(Nullable(UInt32), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_uint32 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_uint32 VALUES ((1, 1)), ((2, 1));
SELECT 'grid N UInt32/UInt8',
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid N UInt32/UInt16',
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid N UInt32/UInt32',
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid N UInt32/UInt64',
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid N UInt32/Int8',
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid N UInt32/Int16',
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid N UInt32/Int32',
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid N UInt32/Int64',
    (SELECT count() FROM c_gcn_uint32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gcn_uint32; DROP TABLE o_gcn_uint32;
DROP TABLE IF EXISTS c_gcn_uint64; DROP TABLE IF EXISTS o_gcn_uint64;
CREATE TABLE c_gcn_uint64 (kt Tuple(Nullable(UInt64), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_uint64 (kt Tuple(Nullable(UInt64), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_uint64 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_uint64 VALUES ((1, 1)), ((2, 1));
SELECT 'grid N UInt64/UInt8',
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid N UInt64/UInt16',
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid N UInt64/UInt32',
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid N UInt64/UInt64',
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid N UInt64/Int8',
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid N UInt64/Int16',
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid N UInt64/Int32',
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid N UInt64/Int64',
    (SELECT count() FROM c_gcn_uint64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_uint64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_uint64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gcn_uint64; DROP TABLE o_gcn_uint64;
DROP TABLE IF EXISTS c_gcn_int8; DROP TABLE IF EXISTS o_gcn_int8;
CREATE TABLE c_gcn_int8 (kt Tuple(Nullable(Int8), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_int8 (kt Tuple(Nullable(Int8), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_int8 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_int8 VALUES ((1, 1)), ((2, 1));
SELECT 'grid N Int8/UInt8',
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid N Int8/UInt16',
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid N Int8/UInt32',
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid N Int8/UInt64',
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid N Int8/Int8',
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid N Int8/Int16',
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid N Int8/Int32',
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid N Int8/Int64',
    (SELECT count() FROM c_gcn_int8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gcn_int8; DROP TABLE o_gcn_int8;
DROP TABLE IF EXISTS c_gcn_int16; DROP TABLE IF EXISTS o_gcn_int16;
CREATE TABLE c_gcn_int16 (kt Tuple(Nullable(Int16), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_int16 (kt Tuple(Nullable(Int16), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_int16 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_int16 VALUES ((1, 1)), ((2, 1));
SELECT 'grid N Int16/UInt8',
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid N Int16/UInt16',
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid N Int16/UInt32',
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid N Int16/UInt64',
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid N Int16/Int8',
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid N Int16/Int16',
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid N Int16/Int32',
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid N Int16/Int64',
    (SELECT count() FROM c_gcn_int16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gcn_int16; DROP TABLE o_gcn_int16;
DROP TABLE IF EXISTS c_gcn_int32; DROP TABLE IF EXISTS o_gcn_int32;
CREATE TABLE c_gcn_int32 (kt Tuple(Nullable(Int32), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_int32 (kt Tuple(Nullable(Int32), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_int32 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_int32 VALUES ((1, 1)), ((2, 1));
SELECT 'grid N Int32/UInt8',
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid N Int32/UInt16',
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid N Int32/UInt32',
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid N Int32/UInt64',
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid N Int32/Int8',
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid N Int32/Int16',
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid N Int32/Int32',
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid N Int32/Int64',
    (SELECT count() FROM c_gcn_int32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int32 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gcn_int32; DROP TABLE o_gcn_int32;
DROP TABLE IF EXISTS c_gcn_int64; DROP TABLE IF EXISTS o_gcn_int64;
CREATE TABLE c_gcn_int64 (kt Tuple(Nullable(Int64), Nullable(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS allow_nullable_key = 1;
CREATE TABLE o_gcn_int64 (kt Tuple(Nullable(Int64), Nullable(UInt8))) ENGINE = Memory;
INSERT INTO c_gcn_int64 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gcn_int64 VALUES ((1, 1)), ((2, 1));
SELECT 'grid N Int64/UInt8',
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1))));
SELECT 'grid N Int64/UInt16',
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1))));
SELECT 'grid N Int64/UInt32',
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1))));
SELECT 'grid N Int64/UInt64',
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1))));
SELECT 'grid N Int64/Int8',
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1))));
SELECT 'grid N Int64/Int16',
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1))));
SELECT 'grid N Int64/Int32',
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1))));
SELECT 'grid N Int64/Int64',
    (SELECT count() FROM c_gcn_int64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))),
    (SELECT count() FROM c_gcn_int64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gcn_int64 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1))));
DROP TABLE c_gcn_int64; DROP TABLE o_gcn_int64;

SELECT '--- 12x12 integer cross product: every pair stays exact ---';
-- 12x12 integer cross product (arm 2): every pair must stay EXACT.
-- Generated; do not thin to a 'representative' subset.
DROP TABLE IF EXISTS ai_uint8; DROP TABLE IF EXISTS ao_uint8;
CREATE TABLE ai_uint8 (k UInt8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint8 (k UInt8) ENGINE = Memory;
INSERT INTO ai_uint8 VALUES (1), (2);
INSERT INTO ao_uint8 VALUES (1), (2);
SELECT 'arm2 UInt8/UInt8',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 UInt8/UInt16',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 UInt8/UInt32',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 UInt8/UInt64',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 UInt8/UInt128',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 UInt8/UInt256',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 UInt8/Int8',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 UInt8/Int16',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 UInt8/Int32',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 UInt8/Int64',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 UInt8/Int128',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 UInt8/Int256',
    (SELECT count() FROM ai_uint8 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint8 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_uint8 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint8 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_uint8; DROP TABLE ao_uint8;
DROP TABLE IF EXISTS ai_uint16; DROP TABLE IF EXISTS ao_uint16;
CREATE TABLE ai_uint16 (k UInt16) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint16 (k UInt16) ENGINE = Memory;
INSERT INTO ai_uint16 VALUES (1), (2);
INSERT INTO ao_uint16 VALUES (1), (2);
SELECT 'arm2 UInt16/UInt8',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 UInt16/UInt16',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 UInt16/UInt32',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 UInt16/UInt64',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 UInt16/UInt128',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 UInt16/UInt256',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 UInt16/Int8',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 UInt16/Int16',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 UInt16/Int32',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 UInt16/Int64',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 UInt16/Int128',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 UInt16/Int256',
    (SELECT count() FROM ai_uint16 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint16 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_uint16 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint16 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_uint16; DROP TABLE ao_uint16;
DROP TABLE IF EXISTS ai_uint32; DROP TABLE IF EXISTS ao_uint32;
CREATE TABLE ai_uint32 (k UInt32) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint32 (k UInt32) ENGINE = Memory;
INSERT INTO ai_uint32 VALUES (1), (2);
INSERT INTO ao_uint32 VALUES (1), (2);
SELECT 'arm2 UInt32/UInt8',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 UInt32/UInt16',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 UInt32/UInt32',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 UInt32/UInt64',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 UInt32/UInt128',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 UInt32/UInt256',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 UInt32/Int8',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 UInt32/Int16',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 UInt32/Int32',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 UInt32/Int64',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 UInt32/Int128',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 UInt32/Int256',
    (SELECT count() FROM ai_uint32 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint32 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_uint32 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint32 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_uint32; DROP TABLE ao_uint32;
DROP TABLE IF EXISTS ai_uint64; DROP TABLE IF EXISTS ao_uint64;
CREATE TABLE ai_uint64 (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint64 (k UInt64) ENGINE = Memory;
INSERT INTO ai_uint64 VALUES (1), (2);
INSERT INTO ao_uint64 VALUES (1), (2);
SELECT 'arm2 UInt64/UInt8',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 UInt64/UInt16',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 UInt64/UInt32',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 UInt64/UInt64',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 UInt64/UInt128',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 UInt64/UInt256',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 UInt64/Int8',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 UInt64/Int16',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 UInt64/Int32',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 UInt64/Int64',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 UInt64/Int128',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 UInt64/Int256',
    (SELECT count() FROM ai_uint64 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint64 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_uint64 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint64 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_uint64; DROP TABLE ao_uint64;
DROP TABLE IF EXISTS ai_uint128; DROP TABLE IF EXISTS ao_uint128;
CREATE TABLE ai_uint128 (k UInt128) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint128 (k UInt128) ENGINE = Memory;
INSERT INTO ai_uint128 VALUES (1), (2);
INSERT INTO ao_uint128 VALUES (1), (2);
SELECT 'arm2 UInt128/UInt8',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 UInt128/UInt16',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 UInt128/UInt32',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 UInt128/UInt64',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 UInt128/UInt128',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 UInt128/UInt256',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 UInt128/Int8',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 UInt128/Int16',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 UInt128/Int32',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 UInt128/Int64',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 UInt128/Int128',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 UInt128/Int256',
    (SELECT count() FROM ai_uint128 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint128 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_uint128 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint128 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_uint128; DROP TABLE ao_uint128;
DROP TABLE IF EXISTS ai_uint256; DROP TABLE IF EXISTS ao_uint256;
CREATE TABLE ai_uint256 (k UInt256) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_uint256 (k UInt256) ENGINE = Memory;
INSERT INTO ai_uint256 VALUES (1), (2);
INSERT INTO ao_uint256 VALUES (1), (2);
SELECT 'arm2 UInt256/UInt8',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 UInt256/UInt16',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 UInt256/UInt32',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 UInt256/UInt64',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 UInt256/UInt128',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 UInt256/UInt256',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 UInt256/Int8',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 UInt256/Int16',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 UInt256/Int32',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 UInt256/Int64',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 UInt256/Int128',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 UInt256/Int256',
    (SELECT count() FROM ai_uint256 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint256 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_uint256 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_uint256 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_uint256; DROP TABLE ao_uint256;
DROP TABLE IF EXISTS ai_int8; DROP TABLE IF EXISTS ao_int8;
CREATE TABLE ai_int8 (k Int8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int8 (k Int8) ENGINE = Memory;
INSERT INTO ai_int8 VALUES (1), (2);
INSERT INTO ao_int8 VALUES (1), (2);
SELECT 'arm2 Int8/UInt8',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 Int8/UInt16',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 Int8/UInt32',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 Int8/UInt64',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 Int8/UInt128',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 Int8/UInt256',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 Int8/Int8',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 Int8/Int16',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 Int8/Int32',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 Int8/Int64',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 Int8/Int128',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 Int8/Int256',
    (SELECT count() FROM ai_int8 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int8 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_int8 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int8 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_int8; DROP TABLE ao_int8;
DROP TABLE IF EXISTS ai_int16; DROP TABLE IF EXISTS ao_int16;
CREATE TABLE ai_int16 (k Int16) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int16 (k Int16) ENGINE = Memory;
INSERT INTO ai_int16 VALUES (1), (2);
INSERT INTO ao_int16 VALUES (1), (2);
SELECT 'arm2 Int16/UInt8',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 Int16/UInt16',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 Int16/UInt32',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 Int16/UInt64',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 Int16/UInt128',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 Int16/UInt256',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 Int16/Int8',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 Int16/Int16',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 Int16/Int32',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 Int16/Int64',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 Int16/Int128',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 Int16/Int256',
    (SELECT count() FROM ai_int16 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int16 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_int16 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int16 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_int16; DROP TABLE ao_int16;
DROP TABLE IF EXISTS ai_int32; DROP TABLE IF EXISTS ao_int32;
CREATE TABLE ai_int32 (k Int32) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int32 (k Int32) ENGINE = Memory;
INSERT INTO ai_int32 VALUES (1), (2);
INSERT INTO ao_int32 VALUES (1), (2);
SELECT 'arm2 Int32/UInt8',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 Int32/UInt16',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 Int32/UInt32',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 Int32/UInt64',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 Int32/UInt128',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 Int32/UInt256',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 Int32/Int8',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 Int32/Int16',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 Int32/Int32',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 Int32/Int64',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 Int32/Int128',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 Int32/Int256',
    (SELECT count() FROM ai_int32 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int32 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_int32 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int32 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_int32; DROP TABLE ao_int32;
DROP TABLE IF EXISTS ai_int64; DROP TABLE IF EXISTS ao_int64;
CREATE TABLE ai_int64 (k Int64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int64 (k Int64) ENGINE = Memory;
INSERT INTO ai_int64 VALUES (1), (2);
INSERT INTO ao_int64 VALUES (1), (2);
SELECT 'arm2 Int64/UInt8',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 Int64/UInt16',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 Int64/UInt32',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 Int64/UInt64',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 Int64/UInt128',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 Int64/UInt256',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 Int64/Int8',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 Int64/Int16',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 Int64/Int32',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 Int64/Int64',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 Int64/Int128',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 Int64/Int256',
    (SELECT count() FROM ai_int64 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int64 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_int64 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int64 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_int64; DROP TABLE ao_int64;
DROP TABLE IF EXISTS ai_int128; DROP TABLE IF EXISTS ao_int128;
CREATE TABLE ai_int128 (k Int128) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int128 (k Int128) ENGINE = Memory;
INSERT INTO ai_int128 VALUES (1), (2);
INSERT INTO ao_int128 VALUES (1), (2);
SELECT 'arm2 Int128/UInt8',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 Int128/UInt16',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 Int128/UInt32',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 Int128/UInt64',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 Int128/UInt128',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 Int128/UInt256',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 Int128/Int8',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 Int128/Int16',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 Int128/Int32',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 Int128/Int64',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 Int128/Int128',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 Int128/Int256',
    (SELECT count() FROM ai_int128 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int128 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_int128 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int128 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_int128; DROP TABLE ao_int128;
DROP TABLE IF EXISTS ai_int256; DROP TABLE IF EXISTS ao_int256;
CREATE TABLE ai_int256 (k Int256) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE ao_int256 (k Int256) ENGINE = Memory;
INSERT INTO ai_int256 VALUES (1), (2);
INSERT INTO ao_int256 VALUES (1), (2);
SELECT 'arm2 Int256/UInt8',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt8(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt8(1)));
SELECT 'arm2 Int256/UInt16',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt16(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt16(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt16(1)));
SELECT 'arm2 Int256/UInt32',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt32(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt32(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt32(1)));
SELECT 'arm2 Int256/UInt64',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt64(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt64(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt64(1)));
SELECT 'arm2 Int256/UInt128',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt128(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt128(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt128(1)));
SELECT 'arm2 Int256/UInt256',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toUInt256(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toUInt256(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toUInt256(1)));
SELECT 'arm2 Int256/Int8',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt8(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt8(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt8(1)));
SELECT 'arm2 Int256/Int16',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt16(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt16(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt16(1)));
SELECT 'arm2 Int256/Int32',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt32(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt32(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt32(1)));
SELECT 'arm2 Int256/Int64',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt64(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt64(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt64(1)));
SELECT 'arm2 Int256/Int128',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt128(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt128(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt128(1)));
SELECT 'arm2 Int256/Int256',
    (SELECT count() FROM ai_int256 WHERE k IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int256 WHERE k IN (SELECT toInt256(1))),
    (SELECT count() FROM ai_int256 WHERE k NOT IN (SELECT toInt256(1))) = (SELECT count() FROM ao_int256 WHERE k NOT IN (SELECT toInt256(1)));
DROP TABLE ai_int256; DROP TABLE ao_int256;

SELECT '--- attribute axis: parameters that `equals` treats as interchangeable stay exact ---';

-- `IDataType::equals` ignores the time zone of `DateTime`/`DateTime64` and the precision of
-- `Decimal`, while `getName` prints all three. Deciding exactness by name would decline these
-- pairs and silently lose pruning for the very common shape of a key that declares a time zone
-- against a set element that does not. Each pair below must keep its atom, and the neighbouring
-- pair that differs in a parameter `equals` DOES compare must still decline.

DROP TABLE IF EXISTS at_dt; DROP TABLE IF EXISTS ao_dt;
CREATE TABLE at_dt (t DateTime('UTC')) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
CREATE TABLE ao_dt (t DateTime('UTC')) ENGINE = Memory;
INSERT INTO at_dt VALUES ('2024-01-01 00:00:00'), ('2024-01-02 00:00:00'), ('2024-01-03 00:00:00');
INSERT INTO ao_dt VALUES ('2024-01-01 00:00:00'), ('2024-01-02 00:00:00'), ('2024-01-03 00:00:00');
SELECT 'attr DateTime(UTC)/DateTime prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr DateTime(UTC)/DateTime',
    (SELECT count() FROM at_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00'))) = (SELECT count() FROM ao_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00'))),
    (SELECT count() FROM at_dt WHERE t NOT IN (SELECT toDateTime('2024-01-01 00:00:00'))) = (SELECT count() FROM ao_dt WHERE t NOT IN (SELECT toDateTime('2024-01-01 00:00:00')));
SELECT 'attr DateTime(UTC)/DateTime(Asia/Istanbul) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr DateTime(UTC)/DateTime(Asia/Istanbul)',
    (SELECT count() FROM at_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul'))) = (SELECT count() FROM ao_dt WHERE t IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul'))),
    (SELECT count() FROM at_dt WHERE t NOT IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul'))) = (SELECT count() FROM ao_dt WHERE t NOT IN (SELECT toDateTime('2024-01-01 00:00:00', 'Asia/Istanbul')));
DROP TABLE at_dt; DROP TABLE ao_dt;

DROP TABLE IF EXISTS at_dt64; DROP TABLE IF EXISTS ao_dt64;
CREATE TABLE at_dt64 (t DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
CREATE TABLE ao_dt64 (t DateTime64(3, 'UTC')) ENGINE = Memory;
INSERT INTO at_dt64 VALUES ('2024-01-01 00:00:00.000'), ('2024-01-02 00:00:00.000'), ('2024-01-03 00:00:00.000');
INSERT INTO ao_dt64 VALUES ('2024-01-01 00:00:00.000'), ('2024-01-02 00:00:00.000'), ('2024-01-03 00:00:00.000');
SELECT 'attr DateTime64(3,UTC)/DateTime64(3) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dt64 WHERE t IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr DateTime64(3,UTC)/DateTime64(3)',
    (SELECT count() FROM at_dt64 WHERE t IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)'))) = (SELECT count() FROM ao_dt64 WHERE t IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)'))),
    (SELECT count() FROM at_dt64 WHERE t NOT IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)'))) = (SELECT count() FROM ao_dt64 WHERE t NOT IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(3)')));
-- scale IS compared by `equals`, so a cross-scale pair must still decline (the axis is not widened).
SELECT 'attr DateTime64(3,UTC)/DateTime64(6) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dt64 WHERE t IN (SELECT CAST('2024-01-01 00:00:00.000', 'DateTime64(6)'))) WHERE explain ILIKE '%in 1-element set%';
DROP TABLE at_dt64; DROP TABLE ao_dt64;

DROP TABLE IF EXISTS at_dec; DROP TABLE IF EXISTS ao_dec;
CREATE TABLE at_dec (d Decimal(10, 2)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
CREATE TABLE ao_dec (d Decimal(10, 2)) ENGINE = Memory;
INSERT INTO at_dec VALUES (1.00), (2.00), (3.00);
INSERT INTO ao_dec VALUES (1.00), (2.00), (3.00);
SELECT 'attr Decimal(10,2)/Decimal(18,2) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dec WHERE d IN (SELECT CAST('1.00', 'Decimal(18,2)'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr Decimal(10,2)/Decimal(18,2)',
    (SELECT count() FROM at_dec WHERE d IN (SELECT CAST('1.00', 'Decimal(18,2)'))) = (SELECT count() FROM ao_dec WHERE d IN (SELECT CAST('1.00', 'Decimal(18,2)'))),
    (SELECT count() FROM at_dec WHERE d NOT IN (SELECT CAST('1.00', 'Decimal(18,2)'))) = (SELECT count() FROM ao_dec WHERE d NOT IN (SELECT CAST('1.00', 'Decimal(18,2)')));
-- `Decimal(20,2)` is a `Decimal128` while `Decimal(10,2)` is a `Decimal64`, so `equals` is false on
-- the differing underlying type and the pair must decline even though only the precision is written.
SELECT 'attr Decimal(10,2)/Decimal(20,2) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_dec WHERE d IN (SELECT CAST('1.00', 'Decimal(20,2)'))) WHERE explain ILIKE '%in 1-element set%';
DROP TABLE at_dec; DROP TABLE ao_dec;

DROP TABLE IF EXISTS at_t64;
CREATE TABLE at_t64 (t Time64(3)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO at_t64 VALUES ('12:00:00.123'), ('13:00:00.000'), ('14:00:00.000');
SELECT 'attr Time64(3)/Time64(3) prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_t64 WHERE t IN (SELECT CAST('12:00:00.123', 'Time64(3)'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr Time64(3)/Time64(6) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_t64 WHERE t IN (SELECT CAST('12:00:00.123', 'Time64(6)'))) WHERE explain ILIKE '%in 1-element set%';
DROP TABLE at_t64;

-- `Bool` is a `DataTypeUInt8` carrying a custom name, so it is `equals`-equal to a plain `UInt8`
-- while its cast wrapper clamps every nonzero value to 1. That is not equality-preserving, so a
-- custom name must still decline in both directions -- and the `UInt8` key direction is a genuine
-- wrong-results carrier, not just a pruning question.
DROP TABLE IF EXISTS at_bool;
CREATE TABLE at_bool (b Bool) ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 1;
INSERT INTO at_bool VALUES (false), (true);
SELECT 'attr Bool/UInt8 declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_bool WHERE b IN (SELECT toUInt8(1))) WHERE explain ILIKE '%in 1-element set%';
DROP TABLE at_bool;

DROP TABLE IF EXISTS at_u8; DROP TABLE IF EXISTS ao_u8;
CREATE TABLE at_u8 (b UInt8) ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 1;
CREATE TABLE ao_u8 (b UInt8) ENGINE = Memory;
INSERT INTO at_u8 VALUES (0), (1), (7);
INSERT INTO ao_u8 VALUES (0), (1), (7);
SELECT 'attr UInt8/Bool declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_u8 WHERE b IN (SELECT CAST(1, 'Bool'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr UInt8/Bool',
    (SELECT count() FROM at_u8 WHERE b IN (SELECT CAST(1, 'Bool'))) = (SELECT count() FROM ao_u8 WHERE b IN (SELECT CAST(1, 'Bool'))),
    (SELECT count() FROM at_u8 WHERE b NOT IN (SELECT CAST(1, 'Bool'))) = (SELECT count() FROM ao_u8 WHERE b NOT IN (SELECT CAST(1, 'Bool')));
DROP TABLE at_u8; DROP TABLE ao_u8;

-- The custom-name check has to recurse, because `Tuple(Bool, UInt8)` and `Tuple(UInt8, UInt8)`
-- differ only in a NESTED custom name and container `equals` compares elements with `equals`.
DROP TABLE IF EXISTS at_tb; DROP TABLE IF EXISTS ao_tb;
CREATE TABLE at_tb (t Tuple(Bool, UInt8)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
CREATE TABLE ao_tb (t Tuple(Bool, UInt8)) ENGINE = Memory;
INSERT INTO at_tb VALUES ((true, 1)), ((false, 2)), ((true, 3));
INSERT INTO ao_tb VALUES ((true, 1)), ((false, 2)), ((true, 3));
SELECT 'attr Tuple(Bool,UInt8)/Tuple(UInt8,UInt8) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_tb WHERE t IN (SELECT tuple(toUInt8(1), toUInt8(1)))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr Tuple(Bool,UInt8)/Tuple(UInt8,UInt8)',
    (SELECT count() FROM at_tb WHERE t IN (SELECT tuple(toUInt8(1), toUInt8(1)))) = (SELECT count() FROM ao_tb WHERE t IN (SELECT tuple(toUInt8(1), toUInt8(1))));
DROP TABLE at_tb; DROP TABLE ao_tb;

DROP TABLE IF EXISTS at_tu; DROP TABLE IF EXISTS ao_tu;
CREATE TABLE at_tu (t Tuple(UInt8, UInt8)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
CREATE TABLE ao_tu (t Tuple(UInt8, UInt8)) ENGINE = Memory;
INSERT INTO at_tu VALUES ((0, 1)), ((1, 1)), ((7, 1));
INSERT INTO ao_tu VALUES ((0, 1)), ((1, 1)), ((7, 1));
SELECT 'attr Tuple(UInt8,UInt8)/Tuple(Bool,UInt8) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_tu WHERE t IN (SELECT tuple(CAST(1, 'Bool'), toUInt8(1)))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr Tuple(UInt8,UInt8)/Tuple(Bool,UInt8)',
    (SELECT count() FROM at_tu WHERE t IN (SELECT tuple(CAST(1, 'Bool'), toUInt8(1)))) = (SELECT count() FROM ao_tu WHERE t IN (SELECT tuple(CAST(1, 'Bool'), toUInt8(1))));
DROP TABLE at_tu; DROP TABLE ao_tu;

SELECT '--- an actual NULL in a nullable set element (the cross-type cast rewrites it to the nested default) ---';

-- A source NULL surviving into the prepared set as the nested default is a SECOND root cause, living in
-- the `Nullable`-source branch that this change does not touch, and it is fixed separately by
-- https://github.com/ClickHouse/ClickHouse/pull/111418. The `transform_null_in = 1` shapes are
-- therefore deliberately NOT asserted here - they belong to that PR's test. What stays is the pair of
-- controls proving this change leaves that branch alone.
DROP TABLE IF EXISTS nn_t; DROP TABLE IF EXISTS nn_o;
CREATE TABLE nn_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE nn_o (k UInt64) ENGINE = Memory;
INSERT INTO nn_t VALUES (0), (1), (2);
INSERT INTO nn_o VALUES (0), (1), (2);

-- Control, NOT a carrier: at the default `transform_null_in = 0` the set itself strips nullability and
-- drops the NULL row (`Set::setHeader`), so the element type reaching the index is a plain `UInt8`, the
-- set is empty and the atom is legitimately exact. It must keep saying `0-element set`.
SELECT 'null-elem NOT IN stays exact', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM nn_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)'))) WHERE explain ILIKE '%0-element set%';
SELECT 'null-elem NOT IN',
    (SELECT count() FROM nn_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)'))) = (SELECT count() FROM nn_o WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')));
DROP TABLE nn_t; DROP TABLE nn_o;

-- Keep-pruning control: the identity arm must be untouched, so a `Nullable(UInt8)` key against a
-- `Nullable(UInt8)` element still prunes even though the element may hold NULL.
DROP TABLE IF EXISTS nk_t; DROP TABLE IF EXISTS nk_o;
CREATE TABLE nk_t (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE nk_o (k Nullable(UInt8)) ENGINE = Memory;
INSERT INTO nk_t VALUES (0), (1), (2);
INSERT INTO nk_o VALUES (0), (1), (2);
SELECT 'null-elem identity keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM nk_t WHERE k IN (SELECT CAST(1, 'Nullable(UInt8)'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'null-elem identity',
    (SELECT count() FROM nk_t WHERE k IN (SELECT CAST(1, 'Nullable(UInt8)'))) = (SELECT count() FROM nk_o WHERE k IN (SELECT CAST(1, 'Nullable(UInt8)'))),
    (SELECT count() FROM nk_t WHERE k NOT IN (SELECT CAST(1, 'Nullable(UInt8)'))) = (SELECT count() FROM nk_o WHERE k NOT IN (SELECT CAST(1, 'Nullable(UInt8)')));
DROP TABLE nk_t; DROP TABLE nk_o;

SELECT '--- the lossy conversion path, not the element type, is what forfeits exactness ---';

-- What forfeits exactness is the CONVERSION, not the element type: a `Nullable` element that can be
-- cast with plain `castColumn` (`canBeSafelyCast`) keeps the prepared set a faithful image and must
-- KEEP pruning, while a cross-type conversion that does not preserve equality must not be claimed
-- exact whatever the element type is. The shapes whose only fault is a source NULL surviving the
-- accurate cast belong to the separate `Nullable`-source root cause fixed by
-- https://github.com/ClickHouse/ClickHouse/pull/111418 and are asserted there, not here.

DROP TABLE IF EXISTS lp_t; DROP TABLE IF EXISTS lp_o;
CREATE TABLE lp_t (k UInt8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE lp_o (k UInt8) ENGINE = Memory;
INSERT INTO lp_t VALUES (0), (1), (2);
INSERT INTO lp_o VALUES (0), (1), (2);

-- A literal array holding both a value and a NULL has element type `Array(Nullable(UInt8))`, which is
-- the minimal form of the family `03733`'s `has([10, 50000, 90000, NULL, NULL], toUInt64(id + 2))`
-- block instantiates. `has` must stay correct as well as `NOT has`.
SELECT 'lossy mixed array NOT has declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM lp_t WHERE NOT has([toUInt8(1), NULL], k)) WHERE explain ILIKE '%element set%';
SELECT 'lossy mixed array NOT has',
    (SELECT count() FROM lp_t WHERE NOT has([toUInt8(1), NULL], k)) = (SELECT count() FROM lp_o WHERE NOT has([toUInt8(1), NULL], k)),
    (SELECT count() FROM lp_t WHERE has([toUInt8(1), NULL], k)) = (SELECT count() FROM lp_o WHERE has([toUInt8(1), NULL], k));
DROP TABLE lp_t; DROP TABLE lp_o;

-- The cross-type mixed array: element type `Array(Nullable(UInt32))` against a `UInt64` key.
DROP TABLE IF EXISTS lw_t; DROP TABLE IF EXISTS lw_o;
CREATE TABLE lw_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE lw_o (k UInt64) ENGINE = Memory;
INSERT INTO lw_t VALUES (0), (1), (2);
INSERT INTO lw_o VALUES (0), (1), (2);
SELECT 'lossy cross-type array NOT has declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM lw_t WHERE NOT has([toUInt32(1), NULL], k)) WHERE explain ILIKE '%element set%';
SELECT 'lossy cross-type array NOT has',
    (SELECT count() FROM lw_t WHERE NOT has([toUInt32(1), NULL], k)) = (SELECT count() FROM lw_o WHERE NOT has([toUInt32(1), NULL], k)),
    (SELECT count() FROM lw_t WHERE has([toUInt32(1), NULL], k)) = (SELECT count() FROM lw_o WHERE has([toUInt32(1), NULL], k));
DROP TABLE lw_t; DROP TABLE lw_o;

-- Keep-pruning side of the same boundary. A NULLABLE key takes the `canBeSafelyCast` exit, so every
-- shape here must still say `element set`; a gate keyed on the element type would decline all of them
-- and silently cost pruning on sound queries.
DROP TABLE IF EXISTS sp8_t; DROP TABLE IF EXISTS sp8_o;
CREATE TABLE sp8_t (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE sp8_o (k Nullable(UInt8)) ENGINE = Memory;
INSERT INTO sp8_t VALUES (0), (1), (2), (NULL);
INSERT INTO sp8_o VALUES (0), (1), (2), (NULL);

-- Strengthens the identity control above, which only ever passed a non-NULL value through the wrapper.
SELECT 'safe identity actual NULL keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sp8_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%element set%';
SELECT 'safe identity actual NULL IN keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sp8_t WHERE k IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%element set%';
SELECT 'safe identity actual NULL',
    (SELECT count() FROM sp8_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) = (SELECT count() FROM sp8_o WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1),
    (SELECT count() FROM sp8_t WHERE k IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) = (SELECT count() FROM sp8_o WHERE k IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1);
DROP TABLE sp8_t; DROP TABLE sp8_o;

-- The same, cross-type: a `Nullable(UInt64)` key against a `Nullable(UInt8)` element. `canBeSafelyCast`
-- holds because the target accepts NULL, so the source NULL is preserved and pruning is sound.
DROP TABLE IF EXISTS sp64_t; DROP TABLE IF EXISTS sp64_o;
CREATE TABLE sp64_t (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE sp64_o (k Nullable(UInt64)) ENGINE = Memory;
INSERT INTO sp64_t VALUES (0), (1), (2), (NULL);
INSERT INTO sp64_o VALUES (0), (1), (2), (NULL);
SELECT 'safe cross-type NOT IN keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sp64_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%element set%';
SELECT 'safe cross-type NOT IN',
    (SELECT count() FROM sp64_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) = (SELECT count() FROM sp64_o WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1);
SELECT 'safe cross-type array NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sp64_t WHERE NOT has([toUInt8(1), NULL], k)) WHERE explain ILIKE '%element set%';
SELECT 'safe cross-type array NOT has',
    (SELECT count() FROM sp64_t WHERE NOT has([toUInt8(1), NULL], k)) = (SELECT count() FROM sp64_o WHERE NOT has([toUInt8(1), NULL], k));
DROP TABLE sp64_t; DROP TABLE sp64_o;

-- A composite nullable key: both tuple elements take the safe exit, so the tuple atom keeps pruning.
DROP TABLE IF EXISTS sptu_t; DROP TABLE IF EXISTS sptu_o;
CREATE TABLE sptu_t (a Nullable(UInt8), b Nullable(UInt8)) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE sptu_o (a Nullable(UInt8), b Nullable(UInt8)) ENGINE = Memory;
INSERT INTO sptu_t VALUES (0, 0), (1, 1), (NULL, 1), (2, NULL);
INSERT INTO sptu_o VALUES (0, 0), (1, 1), (NULL, 1), (2, NULL);
SELECT 'safe nullable tuple keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sptu_t WHERE (a, b) NOT IN (SELECT tuple(CAST(NULL, 'Nullable(UInt8)'), CAST(1, 'Nullable(UInt8)'))) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%element set%';
SELECT 'safe nullable tuple',
    (SELECT count() FROM sptu_t WHERE (a, b) NOT IN (SELECT tuple(CAST(NULL, 'Nullable(UInt8)'), CAST(1, 'Nullable(UInt8)'))) SETTINGS transform_null_in = 1) = (SELECT count() FROM sptu_o WHERE (a, b) NOT IN (SELECT tuple(CAST(NULL, 'Nullable(UInt8)'), CAST(1, 'Nullable(UInt8)'))) SETTINGS transform_null_in = 1);
DROP TABLE sptu_t; DROP TABLE sptu_o;
