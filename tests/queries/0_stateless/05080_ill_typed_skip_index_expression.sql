DROP TABLE IF EXISTS t_create;
DROP TABLE IF EXISTS t_alter;
DROP TABLE IF EXISTS t_modify;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_literal;
DROP TABLE IF EXISTS t_arith;
DROP TABLE IF EXISTS t_wrapped;
DROP TABLE IF EXISTS t_unfoldable;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_lc_nullable;

SELECT 'A CREATE TABLE declaring an index expression that cannot be evaluated';
CREATE TABLE t_create (c0 String, c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1; -- { serverError NO_COMMON_TYPE }

SELECT 'B ALTER TABLE ADD INDEX declaring one';
CREATE TABLE t_alter (c0 String, c1 Int8) ENGINE = MergeTree ORDER BY c1;
ALTER TABLE t_alter ADD INDEX i0 c0 = c1 TYPE set(0); -- { serverError NO_COMMON_TYPE }
INSERT INTO t_alter VALUES ('a', 1);
INSERT INTO t_alter VALUES ('b', 2);
OPTIMIZE TABLE t_alter FINAL;
SELECT count() FROM t_alter;

SELECT 'E an ALTER whose own effect makes an existing index unevaluable';
CREATE TABLE t_modify (c0 Int8, c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1;
ALTER TABLE t_modify MODIFY COLUMN c0 String; -- { serverError NO_COMMON_TYPE }
INSERT INTO t_modify VALUES (1, 1);
INSERT INTO t_modify VALUES (2, 2);
OPTIMIZE TABLE t_modify FINAL;
SELECT count() FROM t_modify;

SELECT 'C1 index over a column';
CREATE TABLE t_plain (c0 String, c1 Int8, INDEX i0 c0 TYPE set(0)) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_plain VALUES ('a', 1);
INSERT INTO t_plain VALUES ('b', 2);
OPTIMIZE TABLE t_plain FINAL;
SELECT count() FROM t_plain;

SELECT 'C2 index over a comparison against a literal of the column type';
CREATE TABLE t_literal (c0 String, c1 Int8, INDEX i0 c0 = 'x' TYPE set(0)) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_literal VALUES ('a', 1);
INSERT INTO t_literal VALUES ('x', 2);
OPTIMIZE TABLE t_literal FINAL;
SELECT count() FROM t_literal;

SELECT 'C3 index over arithmetic';
CREATE TABLE t_arith (c0 String, c1 Int8, INDEX i0 c1 + 1 TYPE minmax) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_arith VALUES ('a', 1);
INSERT INTO t_arith VALUES ('b', 2);
OPTIMIZE TABLE t_arith FINAL;
SELECT count() FROM t_arith;

SELECT 'C4 index over a wrapped type';
CREATE TABLE t_wrapped (c0 LowCardinality(Nullable(String)), c1 Int8, INDEX i0 c0 TYPE bloom_filter) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_wrapped VALUES ('a', 1);
INSERT INTO t_wrapped VALUES (NULL, 2);
OPTIMIZE TABLE t_wrapped FINAL;
SELECT count() FROM t_wrapped;

SELECT 'F an expression containing a function that opts out of constant folding is not probed';
CREATE TABLE t_unfoldable (c0 String, c1 Int8, INDEX i0 throwIf(c0 = c1, 'x') TYPE set(0)) ENGINE = MergeTree ORDER BY c1;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_unfoldable';

SELECT 'G LowCardinality alone is probed, a Nullable operand is not';
-- A `Nullable` operand takes `defaultImplementationForNulls`' zero-row early return, so the
-- declaration is not type-checked and stays accepted; the failure still surfaces on `INSERT`.
CREATE TABLE t_lc (c0 LowCardinality(String), c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1; -- { serverError NO_COMMON_TYPE }
CREATE TABLE t_nullable (c0 Nullable(String), c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1;
CREATE TABLE t_lc_nullable (c0 LowCardinality(Nullable(String)), c1 Int8, INDEX i0 c0 = c1 TYPE set(0)) ENGINE = MergeTree ORDER BY c1;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_nullable', 't_lc_nullable');

DROP TABLE t_alter;
DROP TABLE t_modify;
DROP TABLE t_plain;
DROP TABLE t_literal;
DROP TABLE t_arith;
DROP TABLE t_wrapped;
DROP TABLE t_unfoldable;
DROP TABLE t_nullable;
DROP TABLE t_lc_nullable;
