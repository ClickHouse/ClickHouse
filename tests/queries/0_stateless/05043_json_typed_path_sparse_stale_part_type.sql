DROP TABLE IF EXISTS json_sparse_stale_part_type;

CREATE TABLE json_sparse_stale_part_type
(
    id UInt64,
    j Nullable(JSON(x Nullable(String), max_dynamic_paths = 0)),
    materialized UInt64 MATERIALIZED id + 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_stale_part_type
SELECT
    number,
    CAST(if(number = 0, '{"x":"rare"}', '{}'), 'Nullable(JSON(x Nullable(String), max_dynamic_paths = 0))')
FROM numbers(100)
SETTINGS optimize_on_insert = 0;

ALTER TABLE json_sparse_stale_part_type DETACH PART 'all_1_1_0';
ALTER TABLE json_sparse_stale_part_type
    MODIFY COLUMN j JSON(x Nullable(String), y UInt64, max_dynamic_paths = 0) DEFAULT '{}';
ALTER TABLE json_sparse_stale_part_type ATTACH PART 'all_1_1_0';

ALTER TABLE json_sparse_stale_part_type ADD PROJECTION p (SELECT id, j ORDER BY id);
ALTER TABLE json_sparse_stale_part_type
    MATERIALIZE COLUMN materialized,
    MATERIALIZE PROJECTION p
    SETTINGS mutations_sync = 2;

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_sparse_stale_part_type'
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('x', 'y')
ORDER BY tupleElement(path, 1);

SELECT count(), countIf(j.x = 'rare'), sum(j.y), sum(materialized)
FROM json_sparse_stale_part_type;

DROP TABLE json_sparse_stale_part_type;
