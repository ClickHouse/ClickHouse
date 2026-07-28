-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

SET explain_query_plan_default = 'legacy';
SET optimize_use_implicit_projections = 0;

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
