-- Correlation columns that are not part of the sorting key must not enter the patch schema.

SET enable_analyzer = 1;
SET enable_lightweight_update = 1;
SET mutations_sync = 1;

DROP TABLE IF EXISTS t_correlated_update_patch_source;
DROP TABLE IF EXISTS t_correlated_update_patch_target;

CREATE TABLE t_correlated_update_patch_target
(
    id UInt64,
    lookup_key UInt64,
    value Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    patch_parts_version = 'v2',
    apply_patches_on_merge = 0,
    remove_unused_patch_parts = 0;

CREATE TABLE t_correlated_update_patch_source
(
    lookup_key UInt64,
    value Int64
)
ENGINE = MergeTree
ORDER BY lookup_key;

INSERT INTO t_correlated_update_patch_target VALUES
    (1, 100, 0),
    (2, 200, 0);

INSERT INTO t_correlated_update_patch_source VALUES
    (100, 10),
    (200, 20);

UPDATE t_correlated_update_patch_target
SET value =
(
    SELECT s.value
    FROM t_correlated_update_patch_source AS s
    WHERE s.lookup_key = t_correlated_update_patch_target.lookup_key
);

SELECT id, lookup_key, value FROM t_correlated_update_patch_target ORDER BY id;

SELECT count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_correlated_update_patch_target'
  AND active
  AND name LIKE 'patch-%'
  AND column = 'lookup_key';

DROP TABLE t_correlated_update_patch_source;
DROP TABLE t_correlated_update_patch_target;
