DROP TABLE IF EXISTS t_minmax_count_alter;

SET optimize_use_projections = 1;

CREATE TABLE t_minmax_count_alter (carrier UInt64, value UInt64)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_minmax_count_alter VALUES (1, 10), (2, 20);

ALTER TABLE t_minmax_count_alter RENAME COLUMN carrier TO renamed;

SELECT
    (SELECT tuple(min(value), max(value), count()) FROM t_minmax_count_alter SETTINGS optimize_use_implicit_projections = 1)
    = (SELECT tuple(min(value), max(value), count()) FROM t_minmax_count_alter SETTINGS optimize_use_implicit_projections = 0);
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM t_minmax_count_alter SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 1)
WHERE explain ILIKE '%_minmax_count_projection%';

ALTER TABLE t_minmax_count_alter DROP COLUMN renamed;

SELECT
    (SELECT tuple(min(value), max(value), count()) FROM t_minmax_count_alter SETTINGS optimize_use_implicit_projections = 1)
    = (SELECT tuple(min(value), max(value), count()) FROM t_minmax_count_alter SETTINGS optimize_use_implicit_projections = 0);
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM t_minmax_count_alter SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 1)
WHERE explain ILIKE '%_minmax_count_projection%';

DROP TABLE t_minmax_count_alter;
