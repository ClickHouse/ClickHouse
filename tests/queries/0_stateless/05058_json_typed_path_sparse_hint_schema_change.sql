DROP TABLE IF EXISTS json_sparse_hint_schema_change;

CREATE TABLE json_sparse_hint_schema_change
(
    id UInt64,
    j JSON(x LowCardinality(Nullable(String)), y LowCardinality(String), max_dynamic_paths = 1) NULL
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_hint_schema_change
SELECT 1, CAST('{"x":"x","y":"y"}', 'JSON(x LowCardinality(Nullable(String)), y LowCardinality(String), max_dynamic_paths = 1)');

ALTER TABLE json_sparse_hint_schema_change
    MODIFY COLUMN j JSON(x LowCardinality(String), z Nullable(String), max_dynamic_paths = 1) NULL
SETTINGS mutations_sync = 1;

INSERT INTO json_sparse_hint_schema_change
SELECT 2, CAST('{"x":"x","z":"z"}', 'JSON(x LowCardinality(String), z Nullable(String), max_dynamic_paths = 1)');

SELECT count(), countIf(j.x = 'x'), countIf(j.z = 'z')
FROM json_sparse_hint_schema_change;

DROP TABLE json_sparse_hint_schema_change;
