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

SELECT '--- under-approximating carriers (IN over-prunes a live partition) ---';

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
