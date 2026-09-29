-- `NOT NOT c0` is `c0 != 0`: a `UInt8` that is 0 or 1, never the value of `c0` itself. Index
-- analysis used to push an inversion through every `not`, which cancels a pair of them and
-- substitutes the argument for the result. That is sound where the value is only truth-tested,
-- but not where it feeds another function: `(NOT NOT c0) > -0.5` then became `c0 > -0.5` and
-- pruned parts, granules and partitions that do match. Found by SQLancer.

SET allow_experimental_statistics = 1;

DROP TABLE IF EXISTS t_not_stats;
CREATE TABLE t_not_stats (c0 Int64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_not_stats VALUES (0), (0);
INSERT INTO t_not_stats VALUES (-3), (-2), (-1);

SELECT 'statistics';
-- All five rows match: the comparisons are constant over the {0, 1} range of `NOT NOT c0`.
SELECT count() FROM t_not_stats WHERE (NOT (NOT c0)) > -0.5;
SELECT count() FROM t_not_stats WHERE (NOT (NOT c0)) <= 1.5;
SELECT count() FROM t_not_stats WHERE NOT ((NOT (NOT c0)) >= 2);
-- Only the non-zero rows match.
SELECT count() FROM t_not_stats WHERE (NOT (NOT c0)) >= 1;
SELECT sum(c0) FROM t_not_stats WHERE (NOT (NOT c0)) > -0.5;

SELECT 'view';
DROP VIEW IF EXISTS v_not_stats;
CREATE VIEW v_not_stats AS SELECT c0 FROM t_not_stats WHERE (NOT (NOT c0)) > -0.5;
SELECT count() FROM v_not_stats;
SELECT count() FROM v_not_stats WHERE c0 < 0;

SELECT 'primary key';
DROP TABLE IF EXISTS t_not_pk;
CREATE TABLE t_not_pk (c0 Int64) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_not_pk VALUES (-3), (-2), (-1);
INSERT INTO t_not_pk VALUES (0), (0);
INSERT INTO t_not_pk VALUES (1), (2), (3);
SELECT count() FROM t_not_pk WHERE (NOT (NOT c0)) > -0.5;
SELECT count() FROM t_not_pk WHERE (NOT (NOT c0)) <= 1.5;
SELECT count() FROM t_not_pk WHERE NOT ((NOT (NOT c0)) >= 2);
SELECT count() FROM t_not_pk WHERE (NOT (NOT c0)) >= 1;
-- A `NOT` that is only truth-tested is still pushed into the key condition.
SELECT count() FROM t_not_pk WHERE NOT (c0 = 0) SETTINGS force_primary_key = 1;

SELECT 'partition';
DROP TABLE IF EXISTS t_not_part;
CREATE TABLE t_not_part (c0 Int64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY c0;
INSERT INTO t_not_part VALUES (-2), (-1), (0), (1), (2);
SELECT count() FROM t_not_part WHERE (NOT (NOT c0)) > -0.5;
SELECT count() FROM t_not_part WHERE (NOT (NOT c0)) <= 1.5;
SELECT count() FROM t_not_part WHERE NOT ((NOT (NOT c0)) >= 2);
SELECT count() FROM t_not_part WHERE (NOT (NOT c0)) >= 1;

SELECT 'skip index';
-- One row per granule, so a `minmax` skip index can prune down to individual rows.
DROP TABLE IF EXISTS t_not_idx;
CREATE TABLE t_not_idx (c0 Int64, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree
    ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_not_idx VALUES (-2), (-1), (0), (1), (2);
SELECT count() FROM t_not_idx WHERE (NOT (NOT c0)) > -0.5;
SELECT count() FROM t_not_idx WHERE (NOT (NOT c0)) <= 1.5;
SELECT count() FROM t_not_idx WHERE NOT ((NOT (NOT c0)) >= 2);
SELECT count() FROM t_not_idx WHERE (NOT (NOT c0)) >= 1;

SELECT 'unsigned';
-- The same shape on an unsigned key: `c0 <= 1.5` would exclude every stored value.
DROP TABLE IF EXISTS t_not_unsigned;
CREATE TABLE t_not_unsigned (c0 UInt64) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_not_unsigned VALUES (0), (100), (18446744073709551615);
SELECT count() FROM t_not_unsigned WHERE (NOT (NOT c0)) <= 1.5;
SELECT count() FROM t_not_unsigned WHERE (NOT (NOT c0)) > -0.5;

DROP VIEW v_not_stats;
DROP TABLE t_not_stats;
DROP TABLE t_not_pk;
DROP TABLE t_not_part;
DROP TABLE t_not_idx;
DROP TABLE t_not_unsigned;
