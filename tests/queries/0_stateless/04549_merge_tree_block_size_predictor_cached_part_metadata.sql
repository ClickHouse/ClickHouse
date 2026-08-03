SET preferred_block_size_bytes = 1;
SET max_threads = 4;
SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS predictor_metadata_wide;
CREATE TABLE predictor_metadata_wide
(
    id UInt64,
    j JSON(max_dynamic_paths = 4, typed Nullable(String)),
    old_name String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO predictor_metadata_wide VALUES
    (1, '{"typed":"a","untyped":10}', 'x'),
    (2, '{"typed":null,"untyped":20}', 'yy'),
    (3, '{"typed":"c","untyped":30}', 'zzz'),
    (4, '{"typed":"d","untyped":40}', 'wwww');

ALTER TABLE predictor_metadata_wide RENAME COLUMN old_name TO renamed;
ALTER TABLE predictor_metadata_wide
    ADD COLUMN default_value String DEFAULT concat('d', toString(id)),
    ADD COLUMN materialized_value UInt64 MATERIALIZED id + 10;

SET allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0;
SELECT
    'wide sizes off',
    sum(id),
    countIf(isNull(j.typed)),
    sum(length(ifNull(j.typed, ''))),
    sum(toUInt64(j.untyped)),
    sum(length(renamed)),
    sum(length(default_value)),
    sum(materialized_value)
FROM predictor_metadata_wide
PREWHERE id > 0 AND length(renamed) > 0;

SET allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1;
SELECT
    'wide sizes on',
    sum(id),
    countIf(isNull(j.typed)),
    sum(length(ifNull(j.typed, ''))),
    sum(toUInt64(j.untyped)),
    sum(length(renamed)),
    sum(length(default_value)),
    sum(materialized_value)
FROM predictor_metadata_wide
PREWHERE id > 0 AND length(renamed) > 0;

DROP TABLE predictor_metadata_wide;

DROP TABLE IF EXISTS predictor_metadata_compact;
CREATE TABLE predictor_metadata_compact
(
    id UInt64,
    j JSON(max_dynamic_paths = 4, typed Nullable(String)),
    old_name String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO predictor_metadata_compact VALUES
    (1, '{"typed":"a","untyped":10}', 'x'),
    (2, '{"typed":null,"untyped":20}', 'yy'),
    (3, '{"typed":"c","untyped":30}', 'zzz'),
    (4, '{"typed":"d","untyped":40}', 'wwww');

ALTER TABLE predictor_metadata_compact RENAME COLUMN old_name TO renamed;
ALTER TABLE predictor_metadata_compact
    ADD COLUMN default_value String DEFAULT concat('d', toString(id)),
    ADD COLUMN materialized_value UInt64 MATERIALIZED id + 10;

SET allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0;
SELECT
    'compact sizes off',
    sum(id),
    countIf(isNull(j.typed)),
    sum(length(ifNull(j.typed, ''))),
    sum(toUInt64(j.untyped)),
    sum(length(renamed)),
    sum(length(default_value)),
    sum(materialized_value)
FROM predictor_metadata_compact
PREWHERE id > 0 AND length(renamed) > 0;

SET allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1;
SELECT
    'compact sizes on',
    sum(id),
    countIf(isNull(j.typed)),
    sum(length(ifNull(j.typed, ''))),
    sum(toUInt64(j.untyped)),
    sum(length(renamed)),
    sum(length(default_value)),
    sum(materialized_value)
FROM predictor_metadata_compact
PREWHERE id > 0 AND length(renamed) > 0;

DROP TABLE predictor_metadata_compact;
