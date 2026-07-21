-- Projection metadata is re-derived from the projection query at every table load, so an existing
-- projection part may lack a column the current metadata expects (e.g. after ALTER re-points an
-- ALIAS column selected by the projection). Reading or merging such a part must not fill the
-- missing column with defaults: reads fall back to the parent part, merges rebuild the projection.

-- The projections here select an ALIAS column, so building them on INSERT requires the alias to be
-- resolved. Under optimize_respect_aliases=0 that resolution is skipped and every INSERT fails with
-- UNKNOWN_IDENTIFIER, independently of the drift this test exercises (reproduces on master too), so
-- pin the setting away from the randomized value.
-- Random settings limits: optimize_respect_aliases=(1, 1)

DROP TABLE IF EXISTS t_proj_column_drift;

CREATE TABLE t_proj_column_drift
(
    a UInt64,
    b UInt64,
    d UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_column_drift (a, b, d) VALUES (1, 100, 500);

-- the projection part stores the alias source `b`
SELECT 'part columns before drift', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_column_drift' AND active ORDER BY name, column;

-- re-point the alias: the part still stores {a, b}, but the re-derived metadata now expects {a, d}
ALTER TABLE t_proj_column_drift MODIFY COLUMN c UInt64 ALIAS d + 1;

-- the drifted part must be served from the parent (c = 501, not 1 from a default-filled d)
SELECT 'read after drift', a, c FROM t_proj_column_drift ORDER BY a;

-- forcing the projection cannot use the drifted part
SELECT a, c FROM t_proj_column_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- a part written after the alter is not drifted; the mixed read stays correct
INSERT INTO t_proj_column_drift (a, b, d) VALUES (2, 200, 600);

SELECT 'mixed read', a, c FROM t_proj_column_drift ORDER BY a;

-- merging a drifted part must rebuild the projection from the parent data instead of
-- baking default values for the missing column into the merged projection part
OPTIMIZE TABLE t_proj_column_drift FINAL;

SELECT 'read after merge', a, c FROM t_proj_column_drift ORDER BY a;

SELECT a, c FROM t_proj_column_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_column_drift' AND active ORDER BY name, column;

DROP TABLE t_proj_column_drift;

-- must-not-act control (the #108569 scenario): a column added to the table after the part was
-- written is missing from BOTH the projection part and the parent part; the default fill is then
-- correct and the projection must still be usable
DROP TABLE IF EXISTS t_proj_added_column;

CREATE TABLE t_proj_added_column
(
    a UInt64,
    PROJECTION p (SELECT * ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_added_column VALUES (1);

ALTER TABLE t_proj_added_column ADD COLUMN e UInt64 DEFAULT 42;

SELECT 'added column via projection', a, e FROM t_proj_added_column ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_added_column;

-- late-add whose default references a non-stored column (issue #111076): a column added after
-- the part was written is missing from both parts (a legitimate late-add), but its DEFAULT
-- expression references `f`, which the projection part does not store. The default cannot be
-- evaluated on the projection part, so the read must fall back to the parent and the merge must
-- rebuild the projection instead of failing with UNKNOWN_IDENTIFIER on the missing dependency.
DROP TABLE IF EXISTS t_proj_alias_default_drift;

CREATE TABLE t_proj_alias_default_drift
(
    a UInt64,
    b UInt64,
    f UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_alias_default_drift (a, b, f) VALUES (1, 100, 5);

ALTER TABLE t_proj_alias_default_drift ADD COLUMN d UInt64 DEFAULT f * 10;
ALTER TABLE t_proj_alias_default_drift MODIFY COLUMN c UInt64 ALIAS d + 1;

-- the drifted part must be served from the parent (c = d + 1 = f * 10 + 1 = 51)
SELECT 'default-drift read after drift', a, c FROM t_proj_alias_default_drift ORDER BY a;

-- forcing the projection cannot use the drifted part
SELECT a, c FROM t_proj_alias_default_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- merging must rebuild the projection from the parent instead of throwing on the missing `f`
OPTIMIZE TABLE t_proj_alias_default_drift FINAL;

SELECT 'default-drift read after merge', a, c FROM t_proj_alias_default_drift ORDER BY a;

SELECT a, c FROM t_proj_alias_default_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'default-drift part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_alias_default_drift' AND active ORDER BY name, column;

DROP TABLE t_proj_alias_default_drift;

-- same as above but with a MATERIALIZED (not DEFAULT) expression over the non-stored `f`; the fixed
-- path is shared (ColumnDefault::expression), so the guard must route to the parent here too
DROP TABLE IF EXISTS t_proj_mat_default_drift;

CREATE TABLE t_proj_mat_default_drift
(
    a UInt64,
    b UInt64,
    f UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_mat_default_drift (a, b, f) VALUES (1, 100, 7);

ALTER TABLE t_proj_mat_default_drift ADD COLUMN d UInt64 MATERIALIZED f * 10;
ALTER TABLE t_proj_mat_default_drift MODIFY COLUMN c UInt64 ALIAS d + 1;

-- read falls back to the parent (c = d + 1 = f * 10 + 1 = 71); forcing the projection cannot use it
SELECT 'mat-drift read after drift', a, c FROM t_proj_mat_default_drift ORDER BY a;

SELECT a, c FROM t_proj_mat_default_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- merging must rebuild the projection from the parent instead of throwing on the missing `f`
OPTIMIZE TABLE t_proj_mat_default_drift FINAL;

SELECT 'mat-drift read after merge', a, c FROM t_proj_mat_default_drift ORDER BY a;

DROP TABLE t_proj_mat_default_drift;

-- orphaned-but-stored dependency (sibling of #111076): the late-added `d DEFAULT b * 10` references
-- `b`, which the projection part still physically stores but which is no longer a current projection
-- column after the alias `c` is re-pointed off it (the projection now materializes {a, d}). The read
-- path resolves the default against the current projection columns, so `b` is unresolvable there even
-- though it is physically present; the read must fall back to the parent and the merge must rebuild.
DROP TABLE IF EXISTS t_proj_orphaned_stored_dep;

CREATE TABLE t_proj_orphaned_stored_dep
(
    a UInt64,
    b UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

-- the projection part materializes {a, b} (b is the alias source)
INSERT INTO t_proj_orphaned_stored_dep (a, b) VALUES (1, 100);

-- add d (its default references the still-stored b) then re-point c off b onto d: the projection now
-- needs {a, d}, so b stays physically in the part but is no longer a current projection column
ALTER TABLE t_proj_orphaned_stored_dep ADD COLUMN d UInt64 DEFAULT b * 10;
ALTER TABLE t_proj_orphaned_stored_dep MODIFY COLUMN c UInt64 ALIAS d + 1;

-- the drifted part must be served from the parent (c = d + 1 = b * 10 + 1 = 1001)
SELECT 'orphaned-dep read after drift', a, c FROM t_proj_orphaned_stored_dep ORDER BY a;

-- forcing the projection cannot use the drifted part
SELECT a, c FROM t_proj_orphaned_stored_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- merging must rebuild the projection from the parent instead of throwing on the orphaned `b`
OPTIMIZE TABLE t_proj_orphaned_stored_dep FINAL;

SELECT 'orphaned-dep read after merge', a, c FROM t_proj_orphaned_stored_dep ORDER BY a;

SELECT a, c FROM t_proj_orphaned_stored_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_orphaned_stored_dep;

-- unfillable-default drift under deduplicate_merge_projection_mode='ignore': IGNORE tolerates stale
-- projection answers but must not wedge merges. A late-add whose default references a column the
-- projection part cannot resolve (never-stored `f` here) still throws UNKNOWN_IDENTIFIER if its stale
-- part is merged, so this sub-case must rebuild even under IGNORE (other misses stay merge-tolerant).
DROP TABLE IF EXISTS t_proj_ignore_default_drift;

CREATE TABLE t_proj_ignore_default_drift
(
    a UInt64,
    b UInt64,
    f UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a SETTINGS deduplicate_merge_projection_mode = 'ignore';

-- Two parts must reach the tested OPTIMIZE as separate drifted parts, so hold background merges off
-- until the drift is in place; otherwise a background merge could consume them before the OPTIMIZE and
-- the fixed multi-part merge branch would not be exercised.
SYSTEM STOP MERGES t_proj_ignore_default_drift;

INSERT INTO t_proj_ignore_default_drift (a, b, f) VALUES (1, 100, 5);
INSERT INTO t_proj_ignore_default_drift (a, b, f) VALUES (2, 200, 6);

ALTER TABLE t_proj_ignore_default_drift ADD COLUMN d UInt64 DEFAULT f * 10;
ALTER TABLE t_proj_ignore_default_drift MODIFY COLUMN c UInt64 ALIAS d + 1;

-- read falls back to the parent (c = d + 1 = f * 10 + 1)
SELECT 'ignore-drift read after drift', a, c FROM t_proj_ignore_default_drift ORDER BY a;

-- the merge must rebuild instead of throwing UNKNOWN_IDENTIFIER on the missing `f` even under IGNORE
SYSTEM START MERGES t_proj_ignore_default_drift;
OPTIMIZE TABLE t_proj_ignore_default_drift FINAL;

SELECT 'ignore-drift read after merge', a, c FROM t_proj_ignore_default_drift ORDER BY a;

-- the rebuilt projection part materializes the current projection columns {a, d}, not the stale set
SELECT 'ignore-drift part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_ignore_default_drift' AND active ORDER BY name, column;

DROP TABLE t_proj_ignore_default_drift;

-- subcolumn default dependency: the late-added `d DEFAULT n.x` references the subcolumn `n.x` of the
-- tuple `n`, which the projection part stores and which is still a current projection column. The part
-- resolves subcolumns, so the default IS fillable there; the fillability check must accept a stored
-- current subcolumn (not just an exact top-level column) and keep the projection usable rather than
-- routing to the parent and rebuilding on merge.
DROP TABLE IF EXISTS t_proj_subcol_default_dep;

CREATE TABLE t_proj_subcol_default_dep
(
    a UInt64,
    n Tuple(x UInt64, y UInt64),
    c UInt64 ALIAS a + 1,
    PROJECTION p (SELECT a, n, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_subcol_default_dep (a, n) VALUES (1, (100, 200));

-- add d whose default reads the subcolumn n.x (n stays stored and current)
ALTER TABLE t_proj_subcol_default_dep ADD COLUMN d UInt64 DEFAULT n.x;
ALTER TABLE t_proj_subcol_default_dep MODIFY COLUMN c UInt64 ALIAS d + 1;

-- c = d + 1 = n.x + 1 = 101; the projection stays usable because n.x is fillable from the stored n
SELECT 'subcol-dep read after drift', a, c FROM t_proj_subcol_default_dep ORDER BY a;

SELECT 'subcol-dep force projection', a, c FROM t_proj_subcol_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- merging keeps the projection (no needless rebuild) and stays correct
OPTIMIZE TABLE t_proj_subcol_default_dep FINAL;

SELECT 'subcol-dep read after merge', a, c FROM t_proj_subcol_default_dep ORDER BY a;

SELECT 'subcol-dep force projection after merge', a, c FROM t_proj_subcol_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_subcol_default_dep;

-- same but with an array-size subcolumn (`arr.size0`): a fixed physical subcolumn with its own stream,
-- fillable from the stored current `arr`, so the projection must stay usable here too
DROP TABLE IF EXISTS t_proj_arrsize_default_dep;

CREATE TABLE t_proj_arrsize_default_dep
(
    a UInt64,
    arr Array(UInt64),
    c UInt64 ALIAS a + 1,
    PROJECTION p (SELECT a, arr, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_arrsize_default_dep (a, arr) VALUES (1, [10, 20, 30]);

ALTER TABLE t_proj_arrsize_default_dep ADD COLUMN d UInt64 DEFAULT arr.size0;
ALTER TABLE t_proj_arrsize_default_dep MODIFY COLUMN c UInt64 ALIAS d + 1;

-- c = d + 1 = length(arr) + 1 = 4; projection stays usable (arr.size0 fillable from stored arr)
SELECT 'arrsize-dep read after drift', a, c FROM t_proj_arrsize_default_dep ORDER BY a;

SELECT 'arrsize-dep force projection', a, c FROM t_proj_arrsize_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_arrsize_default_dep;

-- aggregate projection drift: the state column name embeds the expanded alias
-- (`sum(plus(b, 1))`), so re-pointing the alias leaves the part without the state column the
-- metadata now expects (`sum(plus(d, 1))`); the parent never stores aggregate states, so the
-- absence must still count as drift
DROP TABLE IF EXISTS t_proj_agg_drift;

CREATE TABLE t_proj_agg_drift
(
    a UInt64,
    b UInt64,
    d UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, sum(c) GROUP BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_agg_drift (a, b, d) VALUES (1, 100, 500);

ALTER TABLE t_proj_agg_drift MODIFY COLUMN c UInt64 ALIAS d + 1;

SELECT 'agg read after drift', a, sum(c) FROM t_proj_agg_drift GROUP BY a ORDER BY a;

SELECT a, sum(c) FROM t_proj_agg_drift GROUP BY a ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

INSERT INTO t_proj_agg_drift (a, b, d) VALUES (2, 200, 600);

OPTIMIZE TABLE t_proj_agg_drift FINAL;

SELECT 'agg read after merge', a, sum(c) FROM t_proj_agg_drift GROUP BY a ORDER BY a;

SELECT a, sum(c) FROM t_proj_agg_drift GROUP BY a ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_agg_drift;

-- virtual-column control: virtuals are provided by the reading step, not stored in the part;
-- requiring one must not disqualify the projection
DROP TABLE IF EXISTS t_proj_virtual;

CREATE TABLE t_proj_virtual
(
    a UInt64,
    b UInt64,
    PROJECTION p (SELECT a, b ORDER BY b)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_virtual VALUES (1, 100);

SELECT 'virtual via projection', a, b, _part != '' FROM t_proj_virtual WHERE b = 100
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_virtual;
