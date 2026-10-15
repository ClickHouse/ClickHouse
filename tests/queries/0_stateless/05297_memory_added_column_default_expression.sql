-- A column added to a Memory table by ALTER TABLE ... ADD COLUMN has its DEFAULT or MATERIALIZED expression
-- evaluated for the rows inserted before the ALTER, in reads, filters, row policies and mutations.

DROP TABLE IF EXISTS t_memory_add_default;
CREATE TABLE t_memory_add_default (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_add_default VALUES (1), (2);
ALTER TABLE t_memory_add_default ADD COLUMN c UInt64 DEFAULT k * 2;
ALTER TABLE t_memory_add_default ADD COLUMN d UInt64 DEFAULT c + 1;
ALTER TABLE t_memory_add_default ADD COLUMN m UInt64 MATERIALIZED k + 100;
ALTER TABLE t_memory_add_default ADD COLUMN n Nullable(UInt64) DEFAULT k;
ALTER TABLE t_memory_add_default ADD COLUMN t Tuple(a UInt64, b String) DEFAULT (k * 10, 'x');
ALTER TABLE t_memory_add_default ADD COLUMN e UInt64;
INSERT INTO t_memory_add_default (k) VALUES (3);
ALTER TABLE t_memory_add_default ADD COLUMN x Nullable(UInt64);

SELECT 'read';
SELECT k, c, d, m, n, t, e FROM t_memory_add_default ORDER BY k;
SELECT 'the inputs of the expression are not read by the query';
SELECT c FROM t_memory_add_default ORDER BY c;
SELECT d FROM t_memory_add_default ORDER BY d;
SELECT t.a FROM t_memory_add_default ORDER BY t.a;
SELECT 'a subcolumn of a column without a default';
SELECT k, c, x, x.null FROM t_memory_add_default ORDER BY k;
SELECT 'filters';
SELECT k FROM t_memory_add_default WHERE c = 4;
SELECT k, c, t.b FROM t_memory_add_default PREWHERE k >= 2 ORDER BY k;
SELECT count() FROM t_memory_add_default WHERE m > 100;
SELECT 'row policies';
DROP ROW POLICY IF EXISTS policy_05297_c ON t_memory_add_default;
CREATE ROW POLICY policy_05297_c ON t_memory_add_default FOR SELECT USING c < 3 TO CURRENT_USER;
SELECT k FROM t_memory_add_default ORDER BY k;
DROP ROW POLICY policy_05297_c ON t_memory_add_default;
DROP ROW POLICY IF EXISTS policy_05297_k ON t_memory_add_default;
CREATE ROW POLICY policy_05297_k ON t_memory_add_default FOR SELECT USING k >= 2 TO CURRENT_USER;
SELECT k, c FROM t_memory_add_default ORDER BY k;
DROP ROW POLICY policy_05297_k ON t_memory_add_default;
SELECT 'mutation';
ALTER TABLE t_memory_add_default DELETE WHERE k = 1;
SELECT k, c, d FROM t_memory_add_default ORDER BY k;
DROP TABLE t_memory_add_default;

SELECT 'compress';
DROP TABLE IF EXISTS t_memory_add_default_compressed;
CREATE TABLE t_memory_add_default_compressed (k UInt64, s String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_add_default_compressed VALUES (1, 'a'), (2, 'b');
ALTER TABLE t_memory_add_default_compressed ADD COLUMN c String DEFAULT concat(s, toString(k));
SELECT k, c FROM t_memory_add_default_compressed ORDER BY k;
SELECT c FROM t_memory_add_default_compressed PREWHERE k = 2;
DROP TABLE t_memory_add_default_compressed;
DROP TABLE IF EXISTS t_memory_add_default_compressed_const;
CREATE TABLE t_memory_add_default_compressed_const (k UInt64) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_add_default_compressed_const VALUES (1), (2);
ALTER TABLE t_memory_add_default_compressed_const ADD COLUMN c UInt64 DEFAULT 42;
SELECT c FROM t_memory_add_default_compressed_const ORDER BY c;
DROP TABLE t_memory_add_default_compressed_const;

SELECT 'a default reading subcolumns of stored columns';
DROP TABLE IF EXISTS t_memory_add_default_subcolumns;
CREATE TABLE t_memory_add_default_subcolumns (k UInt64, t Tuple(a UInt64, b String), x Nullable(UInt64)) ENGINE = Memory;
INSERT INTO t_memory_add_default_subcolumns VALUES (1, (5, 'q'), NULL), (2, (6, 'r'), 3);
ALTER TABLE t_memory_add_default_subcolumns ADD COLUMN c1 UInt64 DEFAULT t.a + k;
ALTER TABLE t_memory_add_default_subcolumns ADD COLUMN c2 UInt8 DEFAULT x.null;
SELECT k, c1, c2 FROM t_memory_add_default_subcolumns ORDER BY k;
DROP TABLE t_memory_add_default_subcolumns;

SELECT 'a default reading a subcolumn of an added column without a default';
DROP TABLE IF EXISTS t_memory_add_default_missing_input;
CREATE TABLE t_memory_add_default_missing_input (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_add_default_missing_input VALUES (1), (2);
ALTER TABLE t_memory_add_default_missing_input ADD COLUMN t Tuple(a UInt64);
ALTER TABLE t_memory_add_default_missing_input ADD COLUMN c UInt64 DEFAULT t.a + k;
SELECT k, t, c FROM t_memory_add_default_missing_input ORDER BY k;
SELECT k, c FROM t_memory_add_default_missing_input ORDER BY k;
SELECT t.a, c FROM t_memory_add_default_missing_input ORDER BY c;
DROP TABLE t_memory_add_default_missing_input;

SELECT 'a default reading added columns without a default that the query does not read';
DROP TABLE IF EXISTS t_memory_add_default_unread_input;
CREATE TABLE t_memory_add_default_unread_input (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_add_default_unread_input VALUES (1);
ALTER TABLE t_memory_add_default_unread_input ADD COLUMN e Enum8('a' = 1, 'b' = 2), ADD COLUMN x Nullable(UInt64);
ALTER TABLE t_memory_add_default_unread_input ADD COLUMN s String DEFAULT toString(e), ADD COLUMN u UInt8 DEFAULT x.null;
SELECT s, u FROM t_memory_add_default_unread_input;
SELECT s, u FROM t_memory_add_default_unread_input PREWHERE k = 1;
SELECT e, x, s, u FROM t_memory_add_default_unread_input;
DROP TABLE t_memory_add_default_unread_input;

SELECT 'Nested';
DROP TABLE IF EXISTS t_memory_add_default_nested;
CREATE TABLE t_memory_add_default_nested (k UInt64, n Nested(a UInt64)) ENGINE = Memory;
INSERT INTO t_memory_add_default_nested VALUES (1, [1, 2]), (2, [3]);
ALTER TABLE t_memory_add_default_nested ADD COLUMN `n.b` Array(UInt64) DEFAULT arrayMap(x -> x * 100, `n.a`);
SELECT k, n.b FROM t_memory_add_default_nested ORDER BY k;
DROP TABLE t_memory_add_default_nested;

SELECT 'only the rows that passed PREWHERE';
DROP TABLE IF EXISTS t_memory_add_default_div;
CREATE TABLE t_memory_add_default_div (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_add_default_div VALUES (0), (5);
ALTER TABLE t_memory_add_default_div ADD COLUMN q UInt64 DEFAULT intDiv(10, k);
SELECT q FROM t_memory_add_default_div PREWHERE k != 0;
DROP TABLE t_memory_add_default_div;

SELECT 'a stateful default starts over in every block';
DROP TABLE IF EXISTS t_memory_add_default_stateful;
CREATE TABLE t_memory_add_default_stateful (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_add_default_stateful VALUES (1), (2);
INSERT INTO t_memory_add_default_stateful VALUES (3), (4);
ALTER TABLE t_memory_add_default_stateful ADD COLUMN r UInt64 DEFAULT rowNumberInAllBlocks();
-- One reading thread, so both blocks are read one after the other by the same reader.
SELECT k, r FROM t_memory_add_default_stateful ORDER BY k SETTINGS max_threads = 1;
DROP TABLE t_memory_add_default_stateful;

SELECT 'a non-deterministic default has one value for the whole read';
DROP TABLE IF EXISTS t_memory_add_default_now;
CREATE TABLE t_memory_add_default_now (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_add_default_now VALUES (1);
INSERT INTO t_memory_add_default_now VALUES (2);
INSERT INTO t_memory_add_default_now VALUES (3);
INSERT INTO t_memory_add_default_now VALUES (4);
INSERT INTO t_memory_add_default_now VALUES (5);
INSERT INTO t_memory_add_default_now VALUES (6);
INSERT INTO t_memory_add_default_now VALUES (7);
INSERT INTO t_memory_add_default_now VALUES (8);
ALTER TABLE t_memory_add_default_now ADD COLUMN n DateTime64(6) DEFAULT now64(6);
SELECT count(DISTINCT n), min(n) > '2020-01-01' FROM t_memory_add_default_now SETTINGS max_threads = 4;
DROP TABLE t_memory_add_default_now;
