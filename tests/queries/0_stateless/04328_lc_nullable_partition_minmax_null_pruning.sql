-- Regression test for the part-level minmax index losing NULL presence for
-- `LowCardinality(Nullable(...))` columns: `MinMaxIndex::update` only handled `ColumnNullable`,
-- so a real `ColumnLowCardinality` with mixed {non-NULL, NULL} values fell through to
-- `ColumnLowCardinality::getExtremes`, which ignores NULLs. The index then claimed the part
-- has no NULLs and `WHERE c0 IS NULL` could prune a part that actually contains NULL.
-- `ifNull(c0, 1)` maps both 1 and NULL to the same partition, so one part holds mixed values
-- while `c0` is still a minmax-indexed partition key column.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_nullable_prune;

CREATE TABLE t_lc_nullable_prune (c0 LowCardinality(Nullable(Int32)))
ENGINE = MergeTree() ORDER BY tuple() PARTITION BY ifNull(c0, 1);

SELECT '-- mixed part written by insert';

INSERT INTO t_lc_nullable_prune (c0) VALUES (1), (NULL);

SELECT count() FROM t_lc_nullable_prune WHERE c0 IS NULL;
SELECT count() FROM t_lc_nullable_prune WHERE c0 = 1;

SELECT '-- after reloading the minmax index from disk';

DETACH TABLE t_lc_nullable_prune SYNC;
ATTACH TABLE t_lc_nullable_prune;

SELECT count() FROM t_lc_nullable_prune WHERE c0 IS NULL;
SELECT count() FROM t_lc_nullable_prune WHERE c0 = 1;

SELECT '-- after a merge recomputes the minmax index';

INSERT INTO t_lc_nullable_prune (c0) VALUES (1);
OPTIMIZE TABLE t_lc_nullable_prune FINAL;

DETACH TABLE t_lc_nullable_prune SYNC;
ATTACH TABLE t_lc_nullable_prune;

SELECT count() FROM t_lc_nullable_prune WHERE c0 IS NULL;
SELECT count() FROM t_lc_nullable_prune WHERE c0 = 1;

DROP TABLE t_lc_nullable_prune;
