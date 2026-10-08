-- A number literal in arithmetic with a `Decimal` column is a `Float64`, so stored expressions compute the
-- same values as before number literals were typed by their context.

DROP TABLE IF EXISTS t_defaults;
DROP TABLE IF EXISTS t_decimal32;
DROP TABLE IF EXISTS t_mutations;
DROP TABLE IF EXISTS t_source;
DROP TABLE IF EXISTS t_target;
DROP VIEW IF EXISTS v_divide;
DROP VIEW IF EXISTS mv_divide;

CREATE TABLE t_defaults (price Decimal64(2), d Float64 DEFAULT price / 3.0, a Float64 ALIAS price / 3.0, z Float64 DEFAULT price / 0.0, s Float64 DEFAULT price / 0.0003)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_defaults (price) VALUES (1);
SELECT 'defaults', price, d, a, z, s FROM t_defaults;

CREATE TABLE t_decimal32 (p Decimal32(5), m Float64 MATERIALIZED p / 1.5) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_decimal32 (p) VALUES (1);
SELECT 'materialized', p, m FROM t_decimal32;

CREATE TABLE t_mutations (price Decimal64(2), u Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mutations VALUES (1, 0);
ALTER TABLE t_mutations UPDATE u = price / 3.0 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_mutations ADD COLUMN m Float64 DEFAULT price * 1.1;
ALTER TABLE t_mutations MATERIALIZE COLUMN m SETTINGS mutations_sync = 2;
SELECT 'mutations', price, u, m FROM t_mutations;

CREATE TABLE t_source (price Decimal64(2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_target (r Float64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_divide TO t_target AS SELECT price / 3.0 AS r FROM t_source;
CREATE VIEW v_divide AS SELECT price / 3.0 AS r, toTypeName(r) AS type FROM t_source;
INSERT INTO t_source VALUES (1);
SELECT 'materialized view', r FROM t_target;
SELECT 'view', r, type FROM v_divide;

DROP VIEW mv_divide;
DROP VIEW v_divide;
DROP TABLE t_target;
DROP TABLE t_source;
DROP TABLE t_mutations;
DROP TABLE t_decimal32;
DROP TABLE t_defaults;
