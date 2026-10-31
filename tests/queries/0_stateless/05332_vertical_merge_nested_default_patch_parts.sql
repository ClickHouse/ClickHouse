-- Vertical merge of Nested arrays with DEFAULT expressions referencing
-- non-existent sibling columns must not corrupt data.
-- https://github.com/ClickHouse/ClickHouse/issues/86123
-- The cases are split across `04050_vertical_merge_nested_default_corruption`,
-- `05331_vertical_merge_nested_default_mixed_parts` and `05332_vertical_merge_nested_default_patch_parts`
-- to keep each test fast enough.

-- A `Nested` subcolumn whose live values are in a patch part must not be expired. The base parts
-- predate both `m` and `n.b`; `m` is therefore expired, but the lightweight update supplies live
-- `n.b` values in a patch part. The update also writes the matching `n.a` values because nested
-- arrays in an update block must have matching sizes. Expiring `n.b` would omit it from the merge
-- read set and lose those values before `AlterConversions::getPatchesForColumns` can request the
-- patch.
SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_nested_patch_part;

CREATE TABLE t_nested_patch_part (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

INSERT INTO t_nested_patch_part VALUES (1, [10,20]);
INSERT INTO t_nested_patch_part VALUES (2, [30,40]);

ALTER TABLE t_nested_patch_part ADD COLUMN m Array(UInt32);
ALTER TABLE t_nested_patch_part ADD COLUMN `n.b` Array(String) DEFAULT arrayMap(v -> toString(v), m);

UPDATE t_nested_patch_part SET `n.a` = `n.a`, `n.b` = ['x','y'] WHERE id = 1;

OPTIMIZE TABLE t_nested_patch_part FINAL;

SELECT id, `n.a`, `n.b` FROM t_nested_patch_part ORDER BY id;

DROP TABLE t_nested_patch_part;

-- Reader-chain filters must be applied to the cached shared offsets before a missing default is
-- evaluated. The delete filter removes the row whose array has one element, leaving the row whose
-- sibling has two. The constant default has one element, so after the merge it must be reconciled
-- to two type-default values rather than written with incompatible shared Nested offsets.
DROP TABLE IF EXISTS t_nested_filtered_default;

CREATE TABLE t_nested_filtered_default (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_nested_filtered_default VALUES (1, [10]);
INSERT INTO t_nested_filtered_default VALUES (2, [30,40]);

ALTER TABLE t_nested_filtered_default ADD COLUMN `n.b` Array(String) DEFAULT ['x'];

DELETE FROM t_nested_filtered_default WHERE id = 1;
OPTIMIZE TABLE t_nested_filtered_default FINAL;

SELECT id, `n.a`, `n.b` FROM t_nested_filtered_default ORDER BY id;

DROP TABLE t_nested_filtered_default;

-- A merge that expires a column whose `DEFAULT` reads another expired column must be able to fill
-- both. `min_bytes_for_full_part_storage` forces packed part storage, hence a horizontal merge,
-- which is the algorithm that hands the expired columns to the TTL step.
DROP TABLE IF EXISTS t_nested_expired_default_chain;

CREATE TABLE t_nested_expired_default_chain (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_bytes_for_full_part_storage = 1000000000;

SYSTEM STOP MERGES t_nested_expired_default_chain;

INSERT INTO t_nested_expired_default_chain VALUES (1, [10,20]);
INSERT INTO t_nested_expired_default_chain VALUES (2, [30]);

ALTER TABLE t_nested_expired_default_chain ADD COLUMN `n.urls` Array(Array(String));
ALTER TABLE t_nested_expired_default_chain ADD COLUMN `n.domains` Array(Array(String))
    DEFAULT arrayMap(x -> arrayMap(y -> domain(y), CAST(x AS Array(String))), `n.urls`);

SYSTEM START MERGES t_nested_expired_default_chain;
OPTIMIZE TABLE t_nested_expired_default_chain FINAL;

SELECT id, `n.a`, `n.urls`, `n.domains` FROM t_nested_expired_default_chain ORDER BY id;

-- Both added columns stay unmaterialized in the merged part and are recomputed on read.
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested_expired_default_chain' AND active
    AND column IN ('n.urls', 'n.domains');

DROP TABLE t_nested_expired_default_chain;

-- The same reconciliation for a multidimensional array: only the outermost level is shared through
-- the `Nested` offsets, so a constant default with a different number of outer elements degrades to
-- offset-shaped type defaults while the inner levels come from the default itself.
DROP TABLE IF EXISTS t_nested_filtered_default_2d;

CREATE TABLE t_nested_filtered_default_2d (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_nested_filtered_default_2d VALUES (1, [10]);
INSERT INTO t_nested_filtered_default_2d VALUES (2, [30,40]);

ALTER TABLE t_nested_filtered_default_2d ADD COLUMN `n.b` Array(Array(String)) DEFAULT [['x']];

DELETE FROM t_nested_filtered_default_2d WHERE id = 1;
OPTIMIZE TABLE t_nested_filtered_default_2d FINAL;

SELECT id, `n.a`, `n.b` FROM t_nested_filtered_default_2d ORDER BY id;

DROP TABLE t_nested_filtered_default_2d;

-- The alias dependency of `t_nested_alias` (in `04050_vertical_merge_nested_default_corruption`) on a part layout that merges horizontally. A packed part makes
-- `chooseMergeAlgorithm` pick `Horizontal`, and only that branch hands the expired columns to the
-- TTL step, which fills them with the value of their `DEFAULT`. That value is exactly the one that
-- disagrees with the shared `Nested` offsets, and an `ALIAS` dependency is not even resolvable
-- there (an alias column is never part of the block), so a column expired because its `DEFAULT`
-- cannot be materialized must be kept out of that step and recomputed on read instead.
DROP TABLE IF EXISTS t_nested_alias_packed;

CREATE TABLE t_nested_alias_packed (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_bytes_for_full_part_storage = 1000000000;

SYSTEM STOP MERGES t_nested_alias_packed;

INSERT INTO t_nested_alias_packed VALUES (1, [10,20]);
INSERT INTO t_nested_alias_packed VALUES (2, [30,40]);

ALTER TABLE t_nested_alias_packed ADD COLUMN s Array(String);
ALTER TABLE t_nested_alias_packed ADD COLUMN s_alias Array(String) ALIAS s;
ALTER TABLE t_nested_alias_packed ADD COLUMN `n.b` Array(String) DEFAULT s_alias;

SYSTEM START MERGES t_nested_alias_packed;
OPTIMIZE TABLE t_nested_alias_packed FINAL;

SELECT count(), countDistinct(_part) FROM t_nested_alias_packed;
SELECT id, `n.a` FROM t_nested_alias_packed ORDER BY id;

-- `n.b` stays unmaterialized in the merged part and is recomputed on read.
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested_alias_packed' AND active
    AND column = 'n.b';

DROP TABLE t_nested_alias_packed;

-- A projection rebuilt during the merge reads the affected column out of the merged block, and
-- `merge_required_columns` does not cover it. Such a column must stay on the merge path, otherwise
-- nothing produces it and `ProjectionDescription::calculateByQuery` fails with
-- `NOT_FOUND_COLUMN_IN_BLOCK`.
DROP TABLE IF EXISTS t_nested_default_projection;

CREATE TABLE t_nested_default_projection (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_default_projection;

INSERT INTO t_nested_default_projection VALUES (1, [10,20]);
INSERT INTO t_nested_default_projection VALUES (2, [30,40]);

ALTER TABLE t_nested_default_projection ADD COLUMN `n.urls` Array(String);
ALTER TABLE t_nested_default_projection ADD COLUMN `n.domains` Array(String)
    DEFAULT arrayMap(x -> domain(x), `n.urls`);
ALTER TABLE t_nested_default_projection ADD PROJECTION p_domains
    (SELECT id, `n.domains` ORDER BY id);

SYSTEM START MERGES t_nested_default_projection;
OPTIMIZE TABLE t_nested_default_projection FINAL;

SELECT count(), countDistinct(_part) FROM t_nested_default_projection;
SELECT id, `n.a`, `n.urls`, `n.domains` FROM t_nested_default_projection ORDER BY id;

DROP TABLE t_nested_default_projection;

-- The same for a single-column skip index, which is built in the vertical stage from the gathered
-- column: the column must not be erased from `gathering_columns` while the index is still rebuilt.
DROP TABLE IF EXISTS t_nested_default_skip_index;

CREATE TABLE t_nested_default_skip_index (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_default_skip_index;

INSERT INTO t_nested_default_skip_index VALUES (1, [10,20]);
INSERT INTO t_nested_default_skip_index VALUES (2, [30,40]);

ALTER TABLE t_nested_default_skip_index ADD COLUMN `n.urls` Array(String);
ALTER TABLE t_nested_default_skip_index ADD COLUMN `n.domains` Array(String)
    DEFAULT arrayMap(x -> domain(x), `n.urls`);
ALTER TABLE t_nested_default_skip_index ADD INDEX idx_domains `n.domains` TYPE bloom_filter GRANULARITY 1;

SYSTEM START MERGES t_nested_default_skip_index;
OPTIMIZE TABLE t_nested_default_skip_index FINAL;

SELECT count(), countDistinct(_part) FROM t_nested_default_skip_index;
SELECT id, `n.a`, `n.urls`, `n.domains` FROM t_nested_default_skip_index ORDER BY id;

DROP TABLE t_nested_default_skip_index;

-- A part may omit a column with a `MissingColumnInfo` marker (`skip_empty_columns_on_insert`): the
-- merge then fills the type default frozen at write time for that part's rows and does not evaluate
-- the current `DEFAULT`. Here one part lacks `n.b` entirely and another carries a marker for it, so
-- the merge must write `n.b` (a single marker cannot represent both) and keep the frozen `[]` for the
-- marker part's rows while recomputing the other part's rows from the new `DEFAULT`. Expiring `n.b`
-- because its `DEFAULT` now reads the absent `m` would drop the marker, and reads would evaluate the
-- current `DEFAULT` (`['d']`) for the marker part's rows.
DROP TABLE IF EXISTS t_nested_missing_marker;

CREATE TABLE t_nested_missing_marker (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_missing_marker;

INSERT INTO t_nested_missing_marker VALUES (1, [10,20]);

ALTER TABLE t_nested_missing_marker ADD COLUMN `n.b` Array(String);
-- All values of `n.b` (and `n.a`) are type defaults: both are omitted from the part with a marker.
INSERT INTO t_nested_missing_marker VALUES (2, [], []);

ALTER TABLE t_nested_missing_marker ADD COLUMN m Array(String);
ALTER TABLE t_nested_missing_marker MODIFY COLUMN `n.b` Array(String) DEFAULT arrayResize(m, 1, 'd');

SELECT id, `n.a` FROM t_nested_missing_marker ORDER BY id;

SYSTEM START MERGES t_nested_missing_marker;
OPTIMIZE TABLE t_nested_missing_marker FINAL;

SELECT count(), countDistinct(_part) FROM t_nested_missing_marker;
-- `n.b` is materialized by the merge: neither dropped nor left to the marker.
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested_missing_marker' AND active AND column = 'n.b';
SELECT id, `n.a`, `n.b` FROM t_nested_missing_marker ORDER BY id;

DROP TABLE t_nested_missing_marker;

-- A projection rebuilt during the merge must see the same value of an affected column as ordinary
-- reads of the merged part. `m` is absent from every source part and `n.b` reads it through the
-- non-`Nested` intermediate `tmp`: the merge reconciles `n.b` with the shared `Nested` offsets
-- (`['','']`), while recomputing the `DEFAULT` on read would give `[]`. The column is therefore
-- written by the merge instead of being expired.
DROP TABLE IF EXISTS t_nested_default_projection_consistency;

CREATE TABLE t_nested_default_projection_consistency (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_default_projection_consistency;

INSERT INTO t_nested_default_projection_consistency VALUES (1, [10,20]);
INSERT INTO t_nested_default_projection_consistency VALUES (2, [30,40]);

ALTER TABLE t_nested_default_projection_consistency ADD COLUMN m Array(String);
ALTER TABLE t_nested_default_projection_consistency ADD COLUMN tmp Array(String) DEFAULT m;
ALTER TABLE t_nested_default_projection_consistency ADD COLUMN `n.b` Array(String) DEFAULT tmp;
ALTER TABLE t_nested_default_projection_consistency ADD PROJECTION p_b
    (SELECT id, `n.b` ORDER BY id);

SYSTEM START MERGES t_nested_default_projection_consistency;
OPTIMIZE TABLE t_nested_default_projection_consistency FINAL;

SELECT count(), countDistinct(_part) FROM t_nested_default_projection_consistency;
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested_default_projection_consistency' AND active AND column = 'n.b';
SELECT id, `n.a`, `n.b` FROM t_nested_default_projection_consistency ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT id, `n.b` FROM mergeTreeProjection(currentDatabase(), t_nested_default_projection_consistency, p_b) ORDER BY id;
-- Both must agree: no row of the projection differs from the base table.
SELECT count() FROM
(
    SELECT id, `n.b` FROM t_nested_default_projection_consistency SETTINGS optimize_use_projections = 0
    EXCEPT
    SELECT id, `n.b` FROM mergeTreeProjection(currentDatabase(), t_nested_default_projection_consistency, p_b)
);

DROP TABLE t_nested_default_projection_consistency;

-- An intermediate whose values for some rows are in a patch part is not recomputed from its
-- `DEFAULT` for those rows, so the dependency closure must not follow it (mirroring the guard that
-- keeps a patched column from being expired). `m` is absent from every part and expired, `tmp`
-- reads `m` but has live values in the patch part, so `n.b` (which reads `tmp`) is written by the
-- merge instead of being expired.
DROP TABLE IF EXISTS t_nested_patched_intermediate;

CREATE TABLE t_nested_patched_intermediate (
    id UInt32,
    `n.a` Array(UInt32)
) ENGINE = MergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

INSERT INTO t_nested_patched_intermediate VALUES (1, [10,20]);
INSERT INTO t_nested_patched_intermediate VALUES (2, [30,40]);

ALTER TABLE t_nested_patched_intermediate ADD COLUMN m Array(UInt32);
ALTER TABLE t_nested_patched_intermediate ADD COLUMN tmp Array(UInt32) DEFAULT m;
ALTER TABLE t_nested_patched_intermediate ADD COLUMN `n.b` Array(String) DEFAULT arrayMap(v -> toString(v), tmp);

UPDATE t_nested_patched_intermediate SET tmp = [7,8] WHERE id = 1;

OPTIMIZE TABLE t_nested_patched_intermediate FINAL;

SELECT count(), countDistinct(_part) FROM t_nested_patched_intermediate;
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested_patched_intermediate' AND active AND column = 'n.b';
SELECT id, `n.a`, tmp, `n.b` FROM t_nested_patched_intermediate ORDER BY id;

DROP TABLE t_nested_patched_intermediate;
