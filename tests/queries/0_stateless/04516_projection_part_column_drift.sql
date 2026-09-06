-- Projection metadata is re-derived from the projection query at every table load, so an existing
-- projection part may lack a column the current metadata expects (e.g. after ALTER re-points an
-- ALIAS column selected by the projection). Reading or merging such a part must not fill the
-- missing column with defaults: reads fall back to the parent part, merges rebuild the projection.

-- The projections here select an ALIAS column, so building them on INSERT requires the alias to be
-- resolved. Under optimize_respect_aliases=0 that resolution is skipped and every INSERT fails with
-- UNKNOWN_IDENTIFIER, independently of the drift this test exercises (reproduces on master too), so
-- pin the setting away from the randomized value.
-- Random settings limits: optimize_respect_aliases=(1, 1)

-- Read-in-order on the base table would decline the forced projections in this test
-- (`PROJECTION_NOT_USED`), so disable it: plan shape is not this test's subject.
SET optimize_read_in_order = 0;

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

-- a plain read would also be correct if the projection had merely been dropped, so pin that it was
-- rebuilt: it is usable again, materializes the current column set, and no stale part was merged
SELECT 'mat-drift forced after merge', a, c FROM t_proj_mat_default_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'mat-drift part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_mat_default_drift' AND active ORDER BY name, column;

SYSTEM FLUSH LOGS part_log;

SELECT 'mat-drift rebuilt not merged',
       sum(ProfileEvents['MergedProjections']), sum(ProfileEvents['RebuiltProjections']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_mat_default_drift'
  AND event_type = 'MergeParts';

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

-- same, but the missing projection column is one the PARENT part stores. That is an
-- IGNORE-tolerable miss on its own, so the scan must not stop at it: its default still reads the
-- non-stored `f`, and merging the stale part throws UNKNOWN_IDENTIFIER on that dependency.
DROP TABLE IF EXISTS t_proj_ignore_parent_stored_dep;

CREATE TABLE t_proj_ignore_parent_stored_dep
(
    a UInt64,
    b UInt64,
    f UInt64,
    d UInt64 DEFAULT f * 10,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a SETTINGS deduplicate_merge_projection_mode = 'ignore';

SYSTEM STOP MERGES t_proj_ignore_parent_stored_dep;

INSERT INTO t_proj_ignore_parent_stored_dep (a, b, f) VALUES (1, 100, 5);
INSERT INTO t_proj_ignore_parent_stored_dep (a, b, f) VALUES (2, 200, 6);

-- `d` predates the parts, so the parent stores it; re-pointing the alias makes the projection
-- require `d`, which its own part never stored
ALTER TABLE t_proj_ignore_parent_stored_dep MODIFY COLUMN c UInt64 ALIAS d + 1;

SELECT 'ignore-parent-stored read after drift', a, c FROM t_proj_ignore_parent_stored_dep ORDER BY a;

SYSTEM START MERGES t_proj_ignore_parent_stored_dep;
OPTIMIZE TABLE t_proj_ignore_parent_stored_dep FINAL;

SELECT 'ignore-parent-stored read after merge', a, c FROM t_proj_ignore_parent_stored_dep ORDER BY a;

-- reading the parent rows would also succeed if the projection were merely dropped, so pin that the
-- projection was rebuilt and now materializes the current column set
SELECT 'ignore-parent-stored part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_ignore_parent_stored_dep' AND active ORDER BY name, column;

SYSTEM FLUSH LOGS part_log;

SELECT 'ignore-parent-stored rebuilt not merged',
       sum(ProfileEvents['MergedProjections']), sum(ProfileEvents['RebuiltProjections']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_ignore_parent_stored_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_ignore_parent_stored_dep;

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

-- a second part, so the OPTIMIZE below merges two projection parts instead of trivially rewriting one
INSERT INTO t_proj_subcol_default_dep (a, n) VALUES (2, (300, 400));

-- merging keeps the projection (no needless rebuild) and stays correct
OPTIMIZE TABLE t_proj_subcol_default_dep FINAL;

SELECT 'subcol-dep read after merge', a, c FROM t_proj_subcol_default_dep ORDER BY a;

SELECT 'subcol-dep force projection after merge', a, c FROM t_proj_subcol_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- merging and rebuilding yield the same values here, so a result-only assertion cannot tell them
-- apart: assert the discriminating profile events to pin that the projection was merged
SYSTEM FLUSH LOGS part_log;

SELECT 'subcol-dep merged not rebuilt',
       sum(ProfileEvents['MergedProjections']) > 0, sum(ProfileEvents['RebuiltProjections'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_subcol_default_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_subcol_default_dep;

-- the drifted counterpart of the case above (the reason the fillability check must resolve a
-- subcolumn through its column in storage): the projection now requires the SUBCOLUMN `g.x`, whose
-- base `g` is a late-added column with a default reading the non-stored `f`. A subcolumn carries no
-- default of its own, so an exact-name default lookup finds none, wrongly declares `g.x` fillable,
-- and the read throws UNKNOWN_IDENTIFIER on `f` instead of routing to the parent.
DROP TABLE IF EXISTS t_proj_subcol_unfillable_dep;

CREATE TABLE t_proj_subcol_unfillable_dep
(
    a UInt64,
    b UInt64,
    f UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_subcol_unfillable_dep (a, b, f) VALUES (1, 100, 7);

ALTER TABLE t_proj_subcol_unfillable_dep ADD COLUMN g Tuple(x UInt64) DEFAULT tuple(f);
ALTER TABLE t_proj_subcol_unfillable_dep MODIFY COLUMN c UInt64 ALIAS tupleElement(g, 'x') + 1;

-- served from the parent: c = g.x + 1 = f + 1 = 8
SELECT 'subcol-unfillable read after drift', a, c FROM t_proj_subcol_unfillable_dep ORDER BY a;

SELECT a, c FROM t_proj_subcol_unfillable_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- the merge must rebuild rather than throw on the missing `f`
OPTIMIZE TABLE t_proj_subcol_unfillable_dep FINAL;

SELECT 'subcol-unfillable read after merge', a, c FROM t_proj_subcol_unfillable_dep ORDER BY a;

SELECT 'subcol-unfillable forced after merge', a, c FROM t_proj_subcol_unfillable_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'subcol-unfillable part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_subcol_unfillable_dep' AND active ORDER BY name, column;

SYSTEM FLUSH LOGS part_log;

SELECT 'subcol-unfillable rebuilt not merged',
       sum(ProfileEvents['MergedProjections']), sum(ProfileEvents['RebuiltProjections']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_subcol_unfillable_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_subcol_unfillable_dep;

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

INSERT INTO t_proj_arrsize_default_dep (a, arr) VALUES (2, [40, 50]);

OPTIMIZE TABLE t_proj_arrsize_default_dep FINAL;

SELECT 'arrsize-dep read after merge', a, c FROM t_proj_arrsize_default_dep ORDER BY a;

SYSTEM FLUSH LOGS part_log;

SELECT 'arrsize-dep merged not rebuilt',
       sum(ProfileEvents['MergedProjections']) > 0, sum(ProfileEvents['RebuiltProjections'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_arrsize_default_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_arrsize_default_dep;

-- a CHAIN of late-added defaults is fillable: the reader synthesizes a missing dependency from its
-- own default in turn, so `d DEFAULT e + 1` over `e DEFAULT 42` is evaluable on the projection part
-- and the projection must stay usable (a non-recursive fillability check declines it and needlessly
-- rebuilds, which is a silent loss of projection use with unchanged query results)
DROP TABLE IF EXISTS t_proj_chain_default_dep;

CREATE TABLE t_proj_chain_default_dep
(
    a UInt64,
    PROJECTION p (SELECT * ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_chain_default_dep VALUES (1);

ALTER TABLE t_proj_chain_default_dep ADD COLUMN e UInt64 DEFAULT 42;
ALTER TABLE t_proj_chain_default_dep ADD COLUMN d UInt64 DEFAULT e + 1;

SELECT 'chain-dep read', a, d FROM t_proj_chain_default_dep ORDER BY a;

SELECT 'chain-dep force projection', a, d FROM t_proj_chain_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

INSERT INTO t_proj_chain_default_dep (a) VALUES (2);

OPTIMIZE TABLE t_proj_chain_default_dep FINAL;

SELECT 'chain-dep force projection after merge', a, d FROM t_proj_chain_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM FLUSH LOGS part_log;

SELECT 'chain-dep merged not rebuilt',
       sum(ProfileEvents['MergedProjections']) > 0, sum(ProfileEvents['RebuiltProjections'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_chain_default_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_chain_default_dep;

-- two branches of one default sharing a dependency (`d -> {e, f} -> g`): resolving the first branch
-- must not leave `g` marked as in-progress, or the second branch is misread as a cycle and the
-- projection is needlessly abandoned
DROP TABLE IF EXISTS t_proj_shared_default_dep;

CREATE TABLE t_proj_shared_default_dep
(
    a UInt64,
    PROJECTION p (SELECT * ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_shared_default_dep VALUES (1);

ALTER TABLE t_proj_shared_default_dep ADD COLUMN g UInt64 DEFAULT 42;
ALTER TABLE t_proj_shared_default_dep ADD COLUMN e UInt64 DEFAULT g + 1;
ALTER TABLE t_proj_shared_default_dep ADD COLUMN f UInt64 DEFAULT g + 2;
ALTER TABLE t_proj_shared_default_dep ADD COLUMN d UInt64 DEFAULT e + f;

-- d = (g + 1) + (g + 2) = 87
SELECT 'shared-dep force projection', a, d FROM t_proj_shared_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- a second part, so the merge predicate (a separate copy of the same rule) is exercised too
INSERT INTO t_proj_shared_default_dep (a) VALUES (2);

OPTIMIZE TABLE t_proj_shared_default_dep FINAL;

SELECT 'shared-dep force projection after merge', a, d FROM t_proj_shared_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM FLUSH LOGS part_log;

SELECT 'shared-dep merged not rebuilt',
       sum(ProfileEvents['MergedProjections']) > 0, sum(ProfileEvents['RebuiltProjections'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_shared_default_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_shared_default_dep;

-- a lambda's formal parameter is bound during evaluation and is not a column of the table, so it
-- must not be treated as an unresolvable dependency
DROP TABLE IF EXISTS t_proj_lambda_default_dep;

CREATE TABLE t_proj_lambda_default_dep
(
    a UInt64,
    arr Array(UInt64),
    PROJECTION p (SELECT * ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_lambda_default_dep VALUES (1, [1, 2, 3]);

ALTER TABLE t_proj_lambda_default_dep ADD COLUMN d Array(UInt64) DEFAULT arrayMap(x -> x + 1, arr);

SELECT 'lambda-dep force projection', a, d FROM t_proj_lambda_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

INSERT INTO t_proj_lambda_default_dep (a, arr) VALUES (2, [10, 20]);

OPTIMIZE TABLE t_proj_lambda_default_dep FINAL;

SELECT 'lambda-dep force projection after merge', a, d FROM t_proj_lambda_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM FLUSH LOGS part_log;

SELECT 'lambda-dep merged not rebuilt',
       sum(ProfileEvents['MergedProjections']) > 0, sum(ProfileEvents['RebuiltProjections'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_lambda_default_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_lambda_default_dep;

-- the formal's name may also be a table column the projection does not store: masking the formal
-- is what keeps it from being read as a dependency on that column, which would decline the
-- projection even though the default is evaluated from `arr` alone
DROP TABLE IF EXISTS t_proj_lambda_shadow_default_dep;

CREATE TABLE t_proj_lambda_shadow_default_dep
(
    a UInt64,
    x UInt64,
    arr Array(UInt64),
    c UInt64 ALIAS a + 1,
    PROJECTION p (SELECT a, arr, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

SYSTEM STOP MERGES t_proj_lambda_shadow_default_dep;

INSERT INTO t_proj_lambda_shadow_default_dep (a, x, arr) VALUES (1, 5, [1, 2, 3]);
INSERT INTO t_proj_lambda_shadow_default_dep (a, x, arr) VALUES (2, 6, [10, 20]);

ALTER TABLE t_proj_lambda_shadow_default_dep ADD COLUMN d Array(UInt64) DEFAULT arrayMap(x -> x + 1, arr);
ALTER TABLE t_proj_lambda_shadow_default_dep MODIFY COLUMN c UInt64 ALIAS length(d);

SELECT 'lambda-shadow-dep force projection', a, c FROM t_proj_lambda_shadow_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM START MERGES t_proj_lambda_shadow_default_dep;

OPTIMIZE TABLE t_proj_lambda_shadow_default_dep FINAL;

SELECT 'lambda-shadow-dep force projection after merge', a, c FROM t_proj_lambda_shadow_default_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM FLUSH LOGS part_log;

SELECT 'lambda-shadow-dep merged not rebuilt',
       sum(ProfileEvents['MergedProjections']) > 0, sum(ProfileEvents['RebuiltProjections'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_lambda_shadow_default_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_lambda_shadow_default_dep;

-- must-not-act counterpart: a chain that bottoms out in a column the projection part does NOT store
-- is still unfillable, so the read routes to the parent and the merge rebuilds
DROP TABLE IF EXISTS t_proj_chain_unfillable_dep;

CREATE TABLE t_proj_chain_unfillable_dep
(
    a UInt64,
    b UInt64,
    f UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_chain_unfillable_dep (a, b, f) VALUES (1, 100, 5);

ALTER TABLE t_proj_chain_unfillable_dep ADD COLUMN e UInt64 DEFAULT f * 2;
ALTER TABLE t_proj_chain_unfillable_dep ADD COLUMN d UInt64 DEFAULT e + 1;
ALTER TABLE t_proj_chain_unfillable_dep MODIFY COLUMN c UInt64 ALIAS d + 1;

-- c = d + 1 = (f * 2 + 1) + 1 = 12, served from the parent
SELECT 'chain-unfillable read after drift', a, c FROM t_proj_chain_unfillable_dep ORDER BY a;

SELECT a, c FROM t_proj_chain_unfillable_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

OPTIMIZE TABLE t_proj_chain_unfillable_dep FINAL;

SELECT 'chain-unfillable read after merge', a, c FROM t_proj_chain_unfillable_dep ORDER BY a;

SELECT 'chain-unfillable forced after merge', a, c FROM t_proj_chain_unfillable_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'chain-unfillable part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_chain_unfillable_dep' AND active ORDER BY name, column;

SYSTEM FLUSH LOGS part_log;

SELECT 'chain-unfillable rebuilt not merged',
       sum(ProfileEvents['MergedProjections']), sum(ProfileEvents['RebuiltProjections']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_chain_unfillable_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_chain_unfillable_dep;

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

-- a dependency that is a CURRENT projection column but which the PARENT part still stores is drift
-- (case (3)), not a late-add (case (4)): the projection part lacks it, so evaluating the default there
-- would substitute a type default while the parent path reads the real values, and the two paths
-- disagree. Here `d DEFAULT x * 10` becomes required through the alias `c`, while a sibling alias `e`
-- makes `x` a current projection column; the stale part stores neither `d` nor `x`, but the parent
-- stores `x`, so the projection must not be used and the merge must rebuild.
DROP TABLE IF EXISTS t_proj_parent_stored_dep_drift;

CREATE TABLE t_proj_parent_stored_dep_drift
(
    a UInt64,
    x UInt64,
    c UInt64 ALIAS a + 1,
    e UInt64 ALIAS a + 2,
    PROJECTION p (SELECT a, c, e ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

SYSTEM STOP MERGES t_proj_parent_stored_dep_drift;

-- both aliases resolve to `a`, so the projection part materializes {a} only
INSERT INTO t_proj_parent_stored_dep_drift (a, x) VALUES (1, 7);
INSERT INTO t_proj_parent_stored_dep_drift (a, x) VALUES (2, 9);

ALTER TABLE t_proj_parent_stored_dep_drift ADD COLUMN d UInt64 DEFAULT x * 10;
ALTER TABLE t_proj_parent_stored_dep_drift MODIFY COLUMN c UInt64 ALIAS d + 1;
ALTER TABLE t_proj_parent_stored_dep_drift MODIFY COLUMN e UInt64 ALIAS x + 2;

-- served from the parent: c = d + 1 = x * 10 + 1 = 71 / 91 (not 1 from a default-filled x)
SELECT 'parent-stored-dep read after drift', a, c FROM t_proj_parent_stored_dep_drift ORDER BY a;

SELECT a, c FROM t_proj_parent_stored_dep_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- the merge must rebuild from the parent instead of baking the default-filled `x` into the part
SYSTEM START MERGES t_proj_parent_stored_dep_drift;
OPTIMIZE TABLE t_proj_parent_stored_dep_drift FINAL;

SELECT 'parent-stored-dep read after merge', a, c FROM t_proj_parent_stored_dep_drift ORDER BY a;

SELECT 'parent-stored-dep forced after merge', a, c FROM t_proj_parent_stored_dep_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM FLUSH LOGS part_log;

SELECT 'parent-stored-dep rebuilt not merged',
       sum(ProfileEvents['MergedProjections']), sum(ProfileEvents['RebuiltProjections']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_parent_stored_dep_drift'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_parent_stored_dep_drift;

-- the merge-side counterpart of the case above, which is what pins the merge copy of the rule.
-- Under the default mode the read-side check already routes such a query to the parent, so the
-- merged part's contents are not observable; under 'ignore' the stale part IS merged and read, so a
-- merge that treats the parent-stored `x` as fillable bakes a default-filled `x` into the projection
-- part and every later projection read returns `c = 1` instead of `x * 10 + 1`.
DROP TABLE IF EXISTS t_proj_ignore_parent_stored_dep_drift;

CREATE TABLE t_proj_ignore_parent_stored_dep_drift
(
    a UInt64,
    x UInt64,
    c UInt64 ALIAS a + 1,
    e UInt64 ALIAS a + 2,
    PROJECTION p (SELECT a, c, e ORDER BY a)
)
ENGINE = MergeTree ORDER BY a SETTINGS deduplicate_merge_projection_mode = 'ignore';

SYSTEM STOP MERGES t_proj_ignore_parent_stored_dep_drift;

INSERT INTO t_proj_ignore_parent_stored_dep_drift (a, x) VALUES (1, 7);
INSERT INTO t_proj_ignore_parent_stored_dep_drift (a, x) VALUES (2, 9);

ALTER TABLE t_proj_ignore_parent_stored_dep_drift ADD COLUMN d UInt64 DEFAULT x * 10;
ALTER TABLE t_proj_ignore_parent_stored_dep_drift MODIFY COLUMN c UInt64 ALIAS d + 1;
ALTER TABLE t_proj_ignore_parent_stored_dep_drift MODIFY COLUMN e UInt64 ALIAS x + 2;

SYSTEM START MERGES t_proj_ignore_parent_stored_dep_drift;
OPTIMIZE TABLE t_proj_ignore_parent_stored_dep_drift FINAL;

-- the projection was rebuilt, so it materializes the current column set including `x`
SELECT 'ignore-parent-stored-dep part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_ignore_parent_stored_dep_drift' AND active ORDER BY name, column;

-- both paths agree: c = d + 1 = x * 10 + 1 (a stale merge would answer 1 from the projection)
SELECT 'ignore-parent-stored-dep parent path', a, c FROM t_proj_ignore_parent_stored_dep_drift ORDER BY a
SETTINGS optimize_use_projections = 0;

SELECT 'ignore-parent-stored-dep forced after merge', a, c FROM t_proj_ignore_parent_stored_dep_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_ignore_parent_stored_dep_drift;

-- a projection stores only the physical columns its query resolves to, so no projection part holds a
-- selected ALIAS. A default depending on one (`d DEFAULT c + 1`) is never fillable there: the reader
-- resolves it against the projection's own columns and throws UNKNOWN_IDENTIFIER.
DROP TABLE IF EXISTS t_proj_alias_backed_dep;

CREATE TABLE t_proj_alias_backed_dep
(
    a UInt64,
    b UInt64,
    c UInt64 ALIAS b * 10,
    e UInt64 ALIAS a + 1,
    PROJECTION p (SELECT a, b, c, e ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

SYSTEM STOP MERGES t_proj_alias_backed_dep;

-- the alias `c` resolves to `b`, so the projection part materializes {a, b} and never `c` itself
INSERT INTO t_proj_alias_backed_dep (a, b) VALUES (1, 7);

SELECT 'alias-backed-dep part columns before drift', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_alias_backed_dep' AND active ORDER BY name, column;

ALTER TABLE t_proj_alias_backed_dep ADD COLUMN d UInt64 DEFAULT c + 1;
ALTER TABLE t_proj_alias_backed_dep MODIFY COLUMN e UInt64 ALIAS d + 1;

-- served from the parent: e = d + 1 = (b * 10 + 1) + 1 = 72
SELECT 'alias-backed-dep read after drift', a, e FROM t_proj_alias_backed_dep ORDER BY a;

SELECT a, e FROM t_proj_alias_backed_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- `b` being requested alongside does not make `c` resolvable: the default is evaluated against the
-- projection part's own columns, not the query's
SELECT a, b, e FROM t_proj_alias_backed_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- the merge rebuilds, so the rebuilt part materializes `d` (a merge of the stale part would bake in
-- a default-filled `d`) and the projection becomes usable again
SYSTEM START MERGES t_proj_alias_backed_dep;
OPTIMIZE TABLE t_proj_alias_backed_dep FINAL;

SELECT 'alias-backed-dep part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_alias_backed_dep' AND active ORDER BY name, column;

SELECT 'alias-backed-dep parent path after merge', a, e FROM t_proj_alias_backed_dep ORDER BY a
SETTINGS optimize_use_projections = 0;

SELECT 'alias-backed-dep forced after merge', a, e FROM t_proj_alias_backed_dep ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM FLUSH LOGS part_log;

SELECT 'alias-backed-dep rebuilt not merged',
       sum(ProfileEvents['MergedProjections']), sum(ProfileEvents['RebuiltProjections']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_proj_alias_backed_dep'
  AND event_type = 'MergeParts';

DROP TABLE t_proj_alias_backed_dep;
