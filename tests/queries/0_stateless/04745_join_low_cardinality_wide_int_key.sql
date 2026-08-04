SET allow_suspicious_low_cardinality_types = 1;

-- A single LowCardinality key wider than 8 bytes selects a packed keys128/keys256 map, which
-- reads key_sizes to copy the key bytes. Keys must match by value, not degenerate to one bucket.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

-- LowCardinality(UInt128): 1 must match only 1, not 5 or 7.
CREATE TABLE t_l (id LowCardinality(UInt128)) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(UInt128)) ENGINE = Memory;
INSERT INTO t_l VALUES (5), (1);
INSERT INTO t_r VALUES (7), (0), (1);
SELECT 'lc_uint128 using', count() FROM t_l JOIN t_r USING (id);
SELECT 'lc_uint128 on', a.id, b.id FROM t_l a JOIN t_r b ON a.id = b.id ORDER BY a.id, b.id;
SELECT 'lc_uint128 hash', count() FROM t_l JOIN t_r USING (id) SETTINGS join_algorithm = 'hash';
SELECT 'lc_uint128 parallel_hash', count() FROM t_l JOIN t_r USING (id) SETTINGS join_algorithm = 'parallel_hash', max_threads = 8;
SELECT 'lc_uint128 grace_hash', count() FROM t_l JOIN t_r USING (id) SETTINGS join_algorithm = 'grace_hash';
SELECT 'lc_uint128 full_sorting_merge', count() FROM t_l JOIN t_r USING (id) SETTINGS join_algorithm = 'full_sorting_merge';
DROP TABLE t_l;
DROP TABLE t_r;

-- Sibling affected widths.
CREATE TABLE t_l (id LowCardinality(Int128)) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(Int128)) ENGINE = Memory;
INSERT INTO t_l VALUES (5), (1);
INSERT INTO t_r VALUES (7), (0), (1);
SELECT 'lc_int128', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

CREATE TABLE t_l (id LowCardinality(UInt256)) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(UInt256)) ENGINE = Memory;
INSERT INTO t_l VALUES (5), (1);
INSERT INTO t_r VALUES (7), (0), (1);
SELECT 'lc_uint256', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

CREATE TABLE t_l (id LowCardinality(Int256)) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(Int256)) ENGINE = Memory;
INSERT INTO t_l VALUES (5), (1);
INSERT INTO t_r VALUES (7), (0), (1);
SELECT 'lc_int256', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

-- Unaffected shapes: regression guards, all correct before the fix.
CREATE TABLE t_l (id LowCardinality(UInt8)) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(UInt8)) ENGINE = Memory;
INSERT INTO t_l VALUES (5), (1);
INSERT INTO t_r VALUES (7), (0), (1);
SELECT 'lc_uint8', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

CREATE TABLE t_l (id LowCardinality(UInt64)) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(UInt64)) ENGINE = Memory;
INSERT INTO t_l VALUES (5), (1);
INSERT INTO t_r VALUES (7), (0), (1);
SELECT 'lc_uint64', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

CREATE TABLE t_l (id UInt128) ENGINE = Memory;
CREATE TABLE t_r (id UInt128) ENGINE = Memory;
INSERT INTO t_l VALUES (5), (1);
INSERT INTO t_r VALUES (7), (0), (1);
SELECT 'plain_uint128', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

CREATE TABLE t_l (id LowCardinality(String)) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(String)) ENGINE = Memory;
INSERT INTO t_l VALUES ('5'), ('1');
INSERT INTO t_r VALUES ('7'), ('0'), ('1');
SELECT 'lc_string', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

CREATE TABLE t_l (id LowCardinality(UUID)) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(UUID)) ENGINE = Memory;
INSERT INTO t_l VALUES ('00000000-0000-0000-0000-000000000005'), ('00000000-0000-0000-0000-000000000001');
INSERT INTO t_r VALUES ('00000000-0000-0000-0000-000000000007'), ('00000000-0000-0000-0000-000000000000'), ('00000000-0000-0000-0000-000000000001');
SELECT 'lc_uuid', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

CREATE TABLE t_l (id LowCardinality(Nullable(UInt128))) ENGINE = Memory;
CREATE TABLE t_r (id LowCardinality(Nullable(UInt128))) ENGINE = Memory;
INSERT INTO t_l VALUES (5), (1);
INSERT INTO t_r VALUES (7), (0), (1);
SELECT 'lc_nullable_uint128', count() FROM t_l JOIN t_r USING (id);
DROP TABLE t_l;
DROP TABLE t_r;

-- Multi-column key containing a wide LowCardinality column.
CREATE TABLE t_l (a LowCardinality(UInt128), b UInt8) ENGINE = Memory;
CREATE TABLE t_r (a LowCardinality(UInt128), b UInt8) ENGINE = Memory;
INSERT INTO t_l VALUES (5, 1), (1, 2);
INSERT INTO t_r VALUES (7, 1), (0, 2), (1, 2);
SELECT 'lc_uint128_multikey', count() FROM t_l JOIN t_r USING (a, b);
DROP TABLE t_l;
DROP TABLE t_r;

-- ASOF join over a wide LowCardinality equality key.
CREATE TABLE t_l (k LowCardinality(UInt128), t UInt64) ENGINE = Memory;
CREATE TABLE t_r (k LowCardinality(UInt128), t UInt64) ENGINE = Memory;
INSERT INTO t_l VALUES (1, 10), (5, 10);
INSERT INTO t_r VALUES (1, 5), (7, 5);
SELECT 'lc_uint128_asof', a.k, b.k FROM t_l a ASOF JOIN t_r b ON a.k = b.k AND a.t >= b.t ORDER BY a.k;
DROP TABLE t_l;
DROP TABLE t_r;
