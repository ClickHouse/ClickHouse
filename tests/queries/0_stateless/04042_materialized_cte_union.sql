SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

CREATE TABLE tm__fuzz_22 (`x` UInt8) ENGINE = MergeTree ORDER BY x SETTINGS allow_vertical_merges_from_compact_to_wide_parts = 1, allow_summing_columns_in_partition_or_order_key = 1, deduplicate_merge_projection_mode = 'ignore', inactive_parts_to_delay_insert = 21, add_minmax_index_for_string_columns = 1, nullable_serialization_version = 'allow_sparse', min_bytes_for_full_part_storage = 1, add_minmax_index_for_temporal_columns = 1, allow_vertical_merges_from_compact_to_wide_parts = 1, deduplicate_merge_projection_mode = 'throw', merge_max_block_size = 256, allow_experimental_reverse_key = 1, index_granularity = 8192, allow_suspicious_indices = 1, add_minmax_index_for_string_columns = 1, allow_floating_point_partition_key = 1, allow_nullable_key = 1, deduplicate_merge_projection_mode = 'throw', inactive_parts_to_delay_insert = 48, max_avg_part_size_for_too_many_parts = 14872062, index_granularity = 512, min_bytes_for_full_part_storage = 2048, min_bytes_for_wide_part = 1, ratio_of_defaults_for_sparse_serialization = 0.93, allow_nullable_key = 1, min_bytes_for_full_part_storage = 1, allow_coalescing_columns_in_partition_or_order_key = 1, ttl_only_drop_parts = 1, remove_empty_parts = 0, allow_vertical_merges_from_compact_to_wide_parts = 1, inactive_parts_to_delay_insert = 5, ratio_of_defaults_for_sparse_serialization = 0.22, index_granularity = 64, merge_max_block_size = 4;
INSERT INTO tm__fuzz_22 SELECT * FROM numbers(1000000);

WITH t AS MATERIALIZED (
        SELECT number AS c FROM numbers(255) LIMIT 686
    )
SELECT *
FROM tm__fuzz_22
WHERE 
    (x IN (
            (SELECT DISTINCT c FROM t WHERE c = 9223372036854775806 LIMIT 9223372036854775807, 460)
            EXCEPT DISTINCT
            (SELECT DISTINCT c FROM t WHERE c = 9223372036854775806 LIMIT 9223372036854775807, 460)
        )
    )
    OR (x IN (
            SELECT DISTINCT c FROM t WHERE toNullable(257) <=> c LIMIT 66, -1
        )
    )
ORDER BY 1 ASC, x DESC
LIMIT 687;
