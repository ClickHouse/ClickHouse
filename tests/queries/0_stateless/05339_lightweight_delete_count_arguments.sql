SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET optimize_trivial_count_query = 0;
SET optimize_functions_to_subcolumns = 0;
SET optimize_normalize_count_variants = 1;
SET apply_deleted_mask = 1;
SET lightweight_deletes_sync = 2;
SET lightweight_delete_mode = 'alter_update';

DROP TABLE IF EXISTS lwd_count_arguments_wide;
DROP TABLE IF EXISTS lwd_count_arguments_compact;

CREATE TABLE lwd_count_arguments_wide (id UInt64, n Nullable(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1;
CREATE TABLE lwd_count_arguments_compact (id UInt64, n Nullable(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 0,
    exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1;

INSERT INTO lwd_count_arguments_wide VALUES (0, 0), (1, NULL), (2, 2);
INSERT INTO lwd_count_arguments_compact VALUES (0, 0), (1, NULL), (2, 2);
DELETE FROM lwd_count_arguments_wide WHERE id = 0;
DELETE FROM lwd_count_arguments_compact WHERE id = 0;

SELECT 'wide';
-- Bare `count` still uses the implicit projection.
SELECT count() FROM lwd_count_arguments_wide SETTINGS force_optimize_projection_name = '_minmax_count_projection';
SELECT count(id) FROM lwd_count_arguments_wide;
SELECT count(n) FROM lwd_count_arguments_wide;

-- Evaluate arguments only for visible rows, and preserve their exceptions.
SELECT count(throwIf(id = 0)) FROM lwd_count_arguments_wide;
SELECT count(throwIf(id)) FROM lwd_count_arguments_wide; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count(throwIf(id)) FROM lwd_count_arguments_wide SETTINGS optimize_normalize_count_variants = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count(throwIf(id)) FROM lwd_count_arguments_wide SETTINGS optimize_use_implicit_projections = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'compact';
SELECT count() FROM lwd_count_arguments_compact SETTINGS force_optimize_projection_name = '_minmax_count_projection';
SELECT count(id) FROM lwd_count_arguments_compact;
SELECT count(n) FROM lwd_count_arguments_compact;
SELECT count(throwIf(id = 0)) FROM lwd_count_arguments_compact;
SELECT count(throwIf(id)) FROM lwd_count_arguments_compact; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count(throwIf(id)) FROM lwd_count_arguments_compact SETTINGS optimize_normalize_count_variants = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count(throwIf(id)) FROM lwd_count_arguments_compact SETTINGS optimize_use_implicit_projections = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- No argument evaluation is needed when all rows have been deleted.
DELETE FROM lwd_count_arguments_wide WHERE 1;
DELETE FROM lwd_count_arguments_compact WHERE 1;
SELECT count(throwIf(id)) FROM lwd_count_arguments_wide;
SELECT count(throwIf(id)) FROM lwd_count_arguments_compact;

DROP TABLE lwd_count_arguments_wide;
DROP TABLE lwd_count_arguments_compact;
