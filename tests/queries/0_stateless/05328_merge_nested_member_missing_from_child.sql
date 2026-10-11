-- A Nested member that a Merge child does not have reads as arrays of defaults sized like the child's stored members of its group, whatever the query reads.

DROP TABLE IF EXISTS t_05328_m;
DROP TABLE IF EXISTS t_05328_m1;
DROP TABLE IF EXISTS t_05328_ms0;
DROP TABLE IF EXISTS t_05328_mal;
DROP TABLE IF EXISTS t_05328_mmem;
DROP TABLE IF EXISTS t_05328_md;
DROP TABLE IF EXISTS t_05328_mx;
DROP TABLE IF EXISTS t_05328_mx0;
DROP TABLE IF EXISTS t_05328_mrp;
DROP TABLE IF EXISTS t_05328_mal2;
DROP TABLE IF EXISTS t_05328_mq;
DROP TABLE IF EXISTS t_05328_mv;
DROP VIEW IF EXISTS t_05328_v;
DROP TABLE IF EXISTS t_05328_mva;
DROP VIEW IF EXISTS t_05328_va;
DROP TABLE IF EXISTS t_05328_d;
DROP TABLE IF EXISTS t_05328_c;
DROP TABLE IF EXISTS t_05328_f;
DROP TABLE IF EXISTS t_05328_s0;
DROP TABLE IF EXISTS t_05328_al;
DROP TABLE IF EXISTS t_05328_mem;

CREATE TABLE t_05328_c (k UInt8, `n.a` Array(UInt8), `n.c` Array(String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05328_c VALUES (1, [1, 2], ['x', 'y']);
CREATE TABLE t_05328_f (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), `n.c` Array(String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05328_f VALUES (2, [3], [7], ['z']);
CREATE TABLE t_05328_s0 (k UInt8, `n.a` Array(UInt8)) ENGINE = MergeTree ORDER BY k SETTINGS share_nested_offsets = 0;
INSERT INTO t_05328_s0 VALUES (3, [1, 2]);
CREATE TABLE t_05328_al (k UInt8, `n.a` Array(UInt8), y UInt8 ALIAS k + 1) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05328_al VALUES (4, [1, 2]);
CREATE TABLE t_05328_mem (k UInt8, `n.a` Array(UInt8)) ENGINE = Memory;
INSERT INTO t_05328_mem VALUES (5, [1, 2]);
CREATE TABLE t_05328_d AS t_05328_c ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05328_c);

CREATE TABLE t_05328_m (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), `n.c` Array(String))
    ENGINE = Merge(currentDatabase(), '^t_05328_(c|f)$');
CREATE TABLE t_05328_m1 (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), `n.c` Array(String))
    ENGINE = Merge(currentDatabase(), '^t_05328_c$');
CREATE TABLE t_05328_ms0 (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_05328_s0$');
CREATE TABLE t_05328_mal (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_05328_al$');
CREATE TABLE t_05328_mmem (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_05328_mem$');
CREATE TABLE t_05328_md (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), `n.c` Array(String))
    ENGINE = Merge(currentDatabase(), '^t_05328_d$');
CREATE TABLE t_05328_mx (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), `n.c` Array(String),
    `n.d` Array(Decimal(10, 2)), `n.m` Array(Map(String, UInt8))) ENGINE = Merge(currentDatabase(), '^t_05328_(c|d)$');
CREATE TABLE t_05328_mx0 (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_05328_(s0|d)$');
CREATE TABLE t_05328_mrp (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), x Nullable(UInt8)) ENGINE = Merge(currentDatabase(), '^t_05328_c$');
CREATE TABLE t_05328_mal2 (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), y UInt8, x Nullable(UInt8)) ENGINE = Merge(currentDatabase(), '^t_05328_al$');

SELECT 'only the missing member';
SELECT n.b FROM t_05328_m1;

SELECT 'with its subcolumns';
SELECT k, n.b, length(n.b), n.b.size0, n.b.null FROM t_05328_m ORDER BY k SETTINGS optimize_functions_to_subcolumns = 0;
SELECT k, n.b, length(n.b), n.b.size0, n.b.null FROM t_05328_m ORDER BY k SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'with a subcolumn of another member';
SELECT k, n.a.size0, n.b FROM t_05328_m ORDER BY k;

SELECT 'with another member';
SELECT k, n.a, n.b FROM t_05328_m ORDER BY k;

SELECT 'ARRAY JOIN';
SELECT k, b FROM t_05328_m ARRAY JOIN n.b AS b ORDER BY k, b;

SELECT 'WHERE';
SELECT count() FROM t_05328_m WHERE length(n.b) = 2;

SELECT 'share_nested_offsets = 0';
SELECT n.b FROM t_05328_ms0;

SELECT 'child with an ALIAS column';
SELECT n.b FROM t_05328_mal;

SELECT 'ALIAS column read next to two missing columns';
SELECT y, n.b, x FROM t_05328_mal2;
SELECT x, n.b, y FROM t_05328_mal2;

SELECT 'Memory child';
SELECT n.b FROM t_05328_mmem;

SELECT 'View child with a constant member';
CREATE VIEW t_05328_v AS SELECT toUInt8(6) AS k, [toUInt8(1), 2] AS `n.a`;
CREATE TABLE t_05328_mv (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_05328_v$');
SELECT n.b FROM t_05328_mv;
CREATE VIEW t_05328_va (`n.a` Array(UInt8), y UInt8 ALIAS 1) AS SELECT [toUInt8(1), 2] AS `n.a`;
CREATE TABLE t_05328_mva (`n.a` Array(UInt8), `n.b` Array(Nullable(UInt8))) ENGINE = Merge(currentDatabase(), '^t_05328_va$');
SELECT n.b FROM t_05328_mva;

SELECT 'Distributed child';
SELECT n.a, n.b, n.b.size0, n.b.null FROM t_05328_md;
SELECT sum(length(n.b)), count() FROM t_05328_md;

SELECT 'MergeTree and Distributed children';
SELECT sum(length(n.b)), count(), groupArray(n.d), groupArray(n.m) FROM t_05328_mx;

SELECT 'share_nested_offsets = 0 child next to a Distributed child';
SELECT sum(length(n.b)), count() FROM t_05328_mx0;

SELECT 'Quantized member of a Distributed child';
SET enable_quantized_codec = 1;
CREATE TABLE t_05328_mq (k UInt8, `n.a` Array(UInt8), `n.q` Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = Merge(currentDatabase(), '^t_05328_d$');
SELECT n.q, hex(n.q.quantized) FROM t_05328_mq;

SELECT 'row policy on another member';
CREATE ROW POLICY p_05328 ON t_05328_c USING length(n.a) > 0 AS PERMISSIVE TO ALL;
SELECT n.b FROM t_05328_m1;
DROP ROW POLICY p_05328 ON t_05328_c;

SELECT 'row policy on another column next to two missing columns';
CREATE ROW POLICY p_05328_k ON t_05328_c USING k > 0 AS PERMISSIVE TO ALL;
SELECT n.b, x FROM t_05328_mrp;
DROP ROW POLICY p_05328_k ON t_05328_c;

DROP TABLE t_05328_m;
DROP TABLE t_05328_m1;
DROP TABLE t_05328_ms0;
DROP TABLE t_05328_mal;
DROP TABLE t_05328_mmem;
DROP TABLE t_05328_md;
DROP TABLE t_05328_mx;
DROP TABLE t_05328_mx0;
DROP TABLE t_05328_mrp;
DROP TABLE t_05328_mal2;
DROP TABLE t_05328_mq;
DROP TABLE t_05328_mv;
DROP VIEW t_05328_v;
DROP TABLE t_05328_mva;
DROP VIEW t_05328_va;
DROP TABLE t_05328_d;
DROP TABLE t_05328_c;
DROP TABLE t_05328_f;
DROP TABLE t_05328_s0;
DROP TABLE t_05328_al;
DROP TABLE t_05328_mem;
