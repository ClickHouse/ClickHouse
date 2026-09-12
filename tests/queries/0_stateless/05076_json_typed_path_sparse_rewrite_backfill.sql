DROP TABLE IF EXISTS json_sparse_rewrite_backfill;

CREATE TABLE json_sparse_rewrite_backfill
(
    id UInt64,
    j JSON(x Nullable(String), max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    serialization_info_version = 'with_types',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_rewrite_backfill
SELECT
    number,
    CAST(if(number = 0, '{"x":"rare"}', '{}'), 'JSON(x Nullable(String), max_dynamic_paths = 0)')
FROM numbers(100)
SETTINGS optimize_on_insert = 0;

ALTER TABLE json_sparse_rewrite_backfill
    MODIFY SETTING serialization_info_version = 'with_subcolumns';
ALTER TABLE json_sparse_rewrite_backfill REWRITE PARTS SETTINGS mutations_sync = 2;

SELECT subcolumns.serializations[indexOf(subcolumns.names, 'x')]
FROM system.parts_columns
WHERE active
    AND database = currentDatabase()
    AND table = 'json_sparse_rewrite_backfill'
    AND column = 'j';

SELECT count(), countIf(j.x = 'rare')
FROM json_sparse_rewrite_backfill;

DROP TABLE json_sparse_rewrite_backfill;
