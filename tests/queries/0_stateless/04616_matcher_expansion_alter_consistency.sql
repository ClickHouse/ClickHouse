-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- The `force_data_skipping_indices` assertions below check that granules are (or are not)
-- pruned after an index is rebuilt, so they depend on deterministic skip-index granule
-- filtering. Under the parallel-replicas coordinator and randomized (merge-tree) settings the
-- number of pruned granules is not stable, so those settings are disabled for this test.

-- An unrelated ALTER (e.g. ADD COLUMN) can change the expansion of a column matcher inside a
-- stored expression. Existing parts must not silently keep data built from the old expansion:
-- skip indices whose effective expression changes are rebuilt (or the ALTER is rejected,
-- depending on `alter_column_secondary_index_mode`), and `MATERIALIZED` columns whose
-- expansion changes are rematerialized.

SET alter_sync = 2;
SET mutations_sync = 2;

-- 1. A skip index over an `ALIAS` column whose body contains a matcher is rebuilt when
--    `ADD COLUMN` changes the matcher expansion (default REBUILD mode).
SELECT '-- index over matcher alias: rebuilt on ADD COLUMN';
DROP TABLE IF EXISTS t_idx_matcher_alter;
CREATE TABLE t_idx_matcher_alter
(
    a UInt64,
    y UInt64 ALIAS greatest(a, * EXCEPT y),
    INDEX idx y TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100;

INSERT INTO t_idx_matcher_alter SELECT number FROM numbers(1000);

-- `y` was `greatest(a, a)`; after adding `b` it becomes `greatest(a, a, b)` = `a + 1000`.
ALTER TABLE t_idx_matcher_alter ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_idx_matcher_alter' AND command ILIKE '%MATERIALIZE INDEX%';

-- With a stale index this would prune the granule containing a = 999 and return 0.
SELECT count() FROM t_idx_matcher_alter WHERE y = 1999 SETTINGS force_data_skipping_indices = 'idx';

-- A pure column rename does not change the effective index expression (identifiers are
-- renamed on both sides), so it must not trigger another rebuild.
ALTER TABLE t_idx_matcher_alter RENAME COLUMN b TO c;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_idx_matcher_alter' AND command ILIKE '%MATERIALIZE INDEX%';

SELECT count() FROM t_idx_matcher_alter WHERE y = 1999 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_idx_matcher_alter;

-- 2. THROW mode rejects an ALTER that changes the effective index expression through matcher
--    re-expansion, but allows alters that do not change it.
SELECT '-- index over matcher alias: THROW mode';
DROP TABLE IF EXISTS t_idx_matcher_throw;
CREATE TABLE t_idx_matcher_throw
(
    a UInt64,
    y UInt64 ALIAS greatest(a, * EXCEPT y),
    INDEX idx y TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY a SETTINGS alter_column_secondary_index_mode = 'throw';

INSERT INTO t_idx_matcher_throw SELECT number FROM numbers(10);

ALTER TABLE t_idx_matcher_throw ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- A comment change does not affect any index expression, so it is allowed.
ALTER TABLE t_idx_matcher_throw MODIFY COMMENT 'comment';
SELECT count() FROM t_idx_matcher_throw WHERE y = 5 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_idx_matcher_throw;

-- 3. A `MATERIALIZED` column whose matcher expansion changes is rematerialized, so existing
--    parts follow the new expansion and do not diverge from new inserts.
SELECT '-- materialized column with matcher: rematerialized on ADD COLUMN';
DROP TABLE IF EXISTS t_mat_matcher_alter;
CREATE TABLE t_mat_matcher_alter
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m)
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_mat_matcher_alter SELECT number FROM numbers(1000);

-- `m` was `greatest(a, a)` = `a`; after adding `b` it becomes `greatest(a, a, b)` = `a + 1000`.
ALTER TABLE t_mat_matcher_alter ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mat_matcher_alter' AND command ILIKE '%MATERIALIZE COLUMN%';

INSERT INTO t_mat_matcher_alter (a) VALUES (5000);

-- All rows, old and new, must follow the new expansion.
SELECT count() FROM t_mat_matcher_alter WHERE m != a + 1000;
SELECT count() FROM t_mat_matcher_alter WHERE m = a + 1000;

DROP TABLE t_mat_matcher_alter;

-- 4. A `MATERIALIZED` column without matchers is not rematerialized by unrelated alters
--    (ordinary metadata-only semantics are kept).
SELECT '-- materialized column without matcher: no mutation on ADD COLUMN';
DROP TABLE IF EXISTS t_mat_plain_alter;
CREATE TABLE t_mat_plain_alter
(
    a UInt64,
    m UInt64 MATERIALIZED a + 1
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_mat_plain_alter SELECT number FROM numbers(10);

ALTER TABLE t_mat_plain_alter ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mat_plain_alter';

DROP TABLE t_mat_plain_alter;
