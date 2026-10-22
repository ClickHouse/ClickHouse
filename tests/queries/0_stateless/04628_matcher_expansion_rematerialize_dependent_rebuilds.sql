-- Automatic rematerialization of a `MATERIALIZED` column whose matcher expansion changed must
-- keep derived data consistent: skip indices (and projections/statistics) that depend on the
-- rematerialized column are rebuilt, dependents that read the column through a subcolumn path
-- join the rematerialization closure, and ALTERs whose rematerialization could never execute
-- (the expression depends on an `EPHEMERAL` column) are rejected up front.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- skip index on a rematerialized column is rebuilt';
DROP TABLE IF EXISTS t_remat_index;
CREATE TABLE t_remat_index
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m),
    INDEX idx m TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100;

INSERT INTO t_remat_index SELECT number FROM numbers(1000);

-- `ADD COLUMN b` changes the expansion of `* EXCEPT m` and triggers `MATERIALIZE COLUMN m`
-- (old parts get `m = a + 1000`). The minmax index over `m` was built from the old values
-- `[0, 999]`; without a rebuild the forced index would prune away every matching granule.
ALTER TABLE t_remat_index ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM t_remat_index WHERE m = 1999 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_remat_index;

SELECT '-- dependent reading through a subcolumn path joins the closure';
DROP TABLE IF EXISTS t_remat_subcolumn;
CREATE TABLE t_remat_subcolumn
(
    a UInt64,
    m1 Tuple(x UInt64) MATERIALIZED tuple(greatest(a, * EXCEPT m1)),
    m2 UInt64 MATERIALIZED m1.x + 1
) ENGINE = MergeTree ORDER BY a;

-- Computing a `MATERIALIZED` column that reads a subcolumn of another `MATERIALIZED` column
-- during INSERT requires the analyzer (the old analyzer fails to resolve `m1.x` here, with or
-- without matchers). The rematerialization itself does not depend on this setting.
INSERT INTO t_remat_subcolumn (a) SELECT number FROM numbers(3) SETTINGS enable_analyzer = 1;

-- `m1` is rematerialized (new expansion includes `b`), and `m2` reads it as the subcolumn
-- `m1.x`, so it must be rematerialized as well, in a later stage than `m1`.
ALTER TABLE t_remat_subcolumn ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_remat_subcolumn' AND command ILIKE '%MATERIALIZE COLUMN%m2%';

SELECT a, m1.x, m2 FROM t_remat_subcolumn ORDER BY a;

DROP TABLE t_remat_subcolumn;

SELECT '-- expression depending on an EPHEMERAL column: ALTER is rejected up front';
DROP TABLE IF EXISTS t_remat_ephemeral;
CREATE TABLE t_remat_ephemeral
(
    a UInt64,
    e UInt64 EPHEMERAL 7,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m) + e
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_remat_ephemeral (a) VALUES (1);

-- The mutation could never read `e` from old parts, so the ALTER must fail before committing
-- metadata instead of leaving a stuck mutation behind.
ALTER TABLE t_remat_ephemeral ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The table is unchanged and still usable.
SELECT a, m FROM t_remat_ephemeral ORDER BY a;

DROP TABLE t_remat_ephemeral;
