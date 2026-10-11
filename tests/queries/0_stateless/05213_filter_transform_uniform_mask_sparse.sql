DROP TABLE IF EXISTS t_filter_transform_uniform_mask_sparse;

CREATE TABLE t_filter_transform_uniform_mask_sparse
(
    id UInt64,
    u8_all_false UInt8,
    u8_mixed UInt8,
    nullable_all_false Nullable(UInt8),
    nullable_mixed Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1,
         serialization_info_version = 'with_types',
         nullable_serialization_version = 'allow_sparse';

INSERT INTO t_filter_transform_uniform_mask_sparse
SELECT
    number,
    toUInt8(0),
    toUInt8(number = 1),
    if(number = 1, toNullable(toUInt8(0)), CAST(NULL AS Nullable(UInt8))),
    if(number = 1, toNullable(toUInt8(1)), CAST(NULL AS Nullable(UInt8)))
FROM numbers(1, 100);

SELECT count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_filter_transform_uniform_mask_sparse'
  AND active
  AND column IN ('u8_all_false', 'u8_mixed', 'nullable_all_false', 'nullable_mixed')
  AND serialization_kind = 'Sparse';

SELECT count(), sum(id) FROM t_filter_transform_uniform_mask_sparse WHERE u8_all_false SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(id) FROM t_filter_transform_uniform_mask_sparse WHERE u8_mixed SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(id) FROM t_filter_transform_uniform_mask_sparse WHERE nullable_all_false SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(id) FROM t_filter_transform_uniform_mask_sparse WHERE nullable_mixed SETTINGS optimize_move_to_prewhere = 0;

SELECT count(), sum(id) FROM t_filter_transform_uniform_mask_sparse PREWHERE u8_all_false SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(id) FROM t_filter_transform_uniform_mask_sparse PREWHERE u8_mixed SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(id) FROM t_filter_transform_uniform_mask_sparse PREWHERE nullable_all_false SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(id) FROM t_filter_transform_uniform_mask_sparse PREWHERE nullable_mixed SETTINGS optimize_move_to_prewhere = 0;

DROP TABLE t_filter_transform_uniform_mask_sparse;
