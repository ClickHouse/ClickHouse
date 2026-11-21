-- A DEFAULT evaluated for a part that lacks the column it names (or the column of a subcolumn it names)
-- must see that column's own value: its DEFAULT, else its type's default, with the column's type.

DROP TABLE IF EXISTS t_default_missing;
CREATE TABLE t_default_missing (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_default_missing VALUES (1);
ALTER TABLE t_default_missing
    ADD COLUMN e Enum8('a' = 1, 'b' = 2), ADD COLUMN d Date, ADD COLUMN fs FixedString(3), ADD COLUMN dt DateTime('UTC'),
    ADD COLUMN lc LowCardinality(String), ADD COLUMN q QBit(Float32, 4), ADD COLUMN x Nullable(UInt64),
    ADD COLUMN tp Tuple(a UInt64), ADD COLUMN j JSON;
ALTER TABLE t_default_missing
    ADD COLUMN s_e String DEFAULT toString(e), ADD COLUMN s_d String DEFAULT toString(d),
    ADD COLUMN s_fs UInt64 DEFAULT length(fs), ADD COLUMN s_dt UInt16 DEFAULT toYear(dt),
    ADD COLUMN s_lc String DEFAULT toTypeName(lc), ADD COLUMN s_q String DEFAULT toTypeName(q),
    ADD COLUMN s_x UInt8 DEFAULT x.null, ADD COLUMN s_tp UInt64 DEFAULT tp.a + 1, ADD COLUMN s_j String DEFAULT toTypeName(j.a);

-- Each one alone, so the column it names is not read.
SELECT s_e FROM t_default_missing;
SELECT s_d FROM t_default_missing;
SELECT s_fs FROM t_default_missing;
SELECT s_dt FROM t_default_missing;
SELECT s_lc FROM t_default_missing;
SELECT s_q FROM t_default_missing;
SELECT s_x FROM t_default_missing;
SELECT s_tp FROM t_default_missing;
SELECT s_j FROM t_default_missing;

-- The column is read, its subcolumn is not.
SELECT x, s_x, tp, s_tp FROM t_default_missing;

SELECT k FROM t_default_missing PREWHERE s_e = 'a' AND s_x = 1;

-- An INSERT that omits the columns the defaults name.
INSERT INTO t_default_missing (k) VALUES (2);

OPTIMIZE TABLE t_default_missing FINAL;
SELECT k, e, s_e, s_d, s_fs, s_dt, s_lc, s_q, s_x, s_tp, s_j FROM t_default_missing ORDER BY k;
DROP TABLE t_default_missing;

DROP TABLE IF EXISTS t_default_missing_vertical;
CREATE TABLE t_default_missing_vertical (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
             min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_default_missing_vertical VALUES (1);
INSERT INTO t_default_missing_vertical VALUES (2);
ALTER TABLE t_default_missing_vertical
    ADD COLUMN e Enum8('a' = 1, 'b' = 2), ADD COLUMN s String DEFAULT toString(e),
    ADD COLUMN t Tuple(a UInt64) DEFAULT tuple(k * 10), ADD COLUMN c UInt64 DEFAULT t.a + 1;
SELECT s FROM t_default_missing_vertical ORDER BY k;
SELECT c FROM t_default_missing_vertical ORDER BY k;
SELECT t, c FROM t_default_missing_vertical ORDER BY k;

-- The merge stores the values the parts read.
OPTIMIZE TABLE t_default_missing_vertical FINAL;
SELECT k, e, s, t, c FROM t_default_missing_vertical ORDER BY k;
DROP TABLE t_default_missing_vertical;

-- A column with a compound name (a Nested member) that the part lacks.
DROP TABLE IF EXISTS t_default_missing_nested;
CREATE TABLE t_default_missing_nested (k UInt64, n Nested(a UInt64)) ENGINE = MergeTree ORDER BY k
    SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
             min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_default_missing_nested VALUES (1, [10, 20]);
INSERT INTO t_default_missing_nested VALUES (2, [30]);
ALTER TABLE t_default_missing_nested ADD COLUMN n.b Array(UInt64), ADD COLUMN `p.q` UInt64;
ALTER TABLE t_default_missing_nested
    ADD COLUMN c UInt64 DEFAULT length(n.b), ADD COLUMN d UInt64 DEFAULT n.b.size0, ADD COLUMN r UInt64 DEFAULT p.q + 1;
SELECT n.b FROM t_default_missing_nested ORDER BY k;
SELECT c FROM t_default_missing_nested ORDER BY k;
SELECT d FROM t_default_missing_nested ORDER BY k;
SELECT r FROM t_default_missing_nested ORDER BY k;
SELECT n.a, c, d FROM t_default_missing_nested ORDER BY k;
OPTIMIZE TABLE t_default_missing_nested FINAL;
SELECT k, n.a, n.b, c, d, r FROM t_default_missing_nested ORDER BY k;
DROP TABLE t_default_missing_nested;

-- A column with a compound name that has a DEFAULT of its own, read by another DEFAULT, on a part that lacks both.
DROP TABLE IF EXISTS t_default_missing_compound;
CREATE TABLE t_default_missing_compound (k UInt64, n Nested(a UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_default_missing_compound VALUES (1, [10, 20]);
ALTER TABLE t_default_missing_compound
    ADD COLUMN n.b Array(UInt64), ADD COLUMN n.e Array(UInt64) DEFAULT arrayMap(x -> x + 1, n.a),
    ADD COLUMN `p.r` UInt64 DEFAULT k * 100;
ALTER TABLE t_default_missing_compound
    ADD COLUMN c UInt64 DEFAULT length(n.b), ADD COLUMN f UInt64 DEFAULT length(n.e), ADD COLUMN g UInt64 DEFAULT n.e.size0,
    ADD COLUMN h UInt64 DEFAULT p.r + 1, ADD COLUMN m UInt64 DEFAULT arraySum(arrayMap(n -> n.e, [CAST(tuple(5), 'Tuple(e UInt64)')]));
SELECT c FROM t_default_missing_compound;
SELECT f FROM t_default_missing_compound;
SELECT g FROM t_default_missing_compound;
SELECT h FROM t_default_missing_compound;
-- `n` in the lambda is its argument, not the Nested column.
SELECT f, m FROM t_default_missing_compound;
SELECT n.e, f, g FROM t_default_missing_compound;
INSERT INTO t_default_missing_compound (k, n.a) VALUES (2, [30]);
SELECT k, n.b, n.e, c, f, g, h, m FROM t_default_missing_compound ORDER BY k;
OPTIMIZE TABLE t_default_missing_compound FINAL;
SELECT k, n.b, n.e, c, f, g, h, m FROM t_default_missing_compound ORDER BY k;
DROP TABLE t_default_missing_compound;
