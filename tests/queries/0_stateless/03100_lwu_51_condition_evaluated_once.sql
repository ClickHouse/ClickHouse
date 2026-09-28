-- Regression test for #86032: the lightweight update/delete condition must not
-- be evaluated again after the patch part prefilter.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_cond_once;

CREATE TABLE t_lwu_cond_once (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, remove_unused_patch_parts = 0;

INSERT INTO t_lwu_cond_once SELECT number FROM numbers(1000);

SET lightweight_delete_mode = 'lightweight_update_force';
DELETE FROM t_lwu_cond_once WHERE rand() % 2 = 0;

SELECT
    deleted_rows = patch_rows AS deleted_matches_patch,
    patch_rows > 0 AS patch_part_written
FROM
(
    SELECT
        1000 - (SELECT count() FROM t_lwu_cond_once) AS deleted_rows,
        (SELECT sum(rows) FROM system.parts
         WHERE database = currentDatabase() AND table = 't_lwu_cond_once'
           AND active AND startsWith(name, 'patch')) AS patch_rows
);

DROP TABLE t_lwu_cond_once;

-- Same invariant for lightweight UPDATE.

DROP TABLE IF EXISTS t_lwu_cond_once_upd;

CREATE TABLE t_lwu_cond_once_upd (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, remove_unused_patch_parts = 0;

INSERT INTO t_lwu_cond_once_upd SELECT number, 0 FROM numbers(1000);

UPDATE t_lwu_cond_once_upd SET b = 1 WHERE rand() % 2 = 0;

SELECT
    changed_rows = patch_rows AS changed_matches_patch,
    patch_rows > 0 AS patch_part_written
FROM
(
    SELECT
        (SELECT countIf(b = 1) FROM t_lwu_cond_once_upd) AS changed_rows,
        (SELECT sum(rows) FROM system.parts
         WHERE database = currentDatabase() AND table = 't_lwu_cond_once_upd'
           AND active AND startsWith(name, 'patch')) AS patch_rows
);

DROP TABLE t_lwu_cond_once_upd;

-- Nested array-size validation must still run when the prefilter has already
-- checked the update condition.

DROP TABLE IF EXISTS t_lwu_cond_once_nested;

CREATE TABLE t_lwu_cond_once_nested
(
    id UInt64,
    n Nested(a UInt64, b String)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_cond_once_nested VALUES (1, [1], ['x']);

UPDATE t_lwu_cond_once_nested SET `n.a` = [10, 20] WHERE id = 1; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
UPDATE t_lwu_cond_once_nested SET `n.a` = [10] WHERE id = 1;
UPDATE t_lwu_cond_once_nested SET `n.a` = arrayMap(x -> x - 1, `n.a`) WHERE id = 1;

SELECT `n.a`, `n.b` FROM t_lwu_cond_once_nested;

DROP TABLE t_lwu_cond_once_nested;
