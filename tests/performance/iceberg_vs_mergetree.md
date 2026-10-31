# Iceberg vs MergeTree in the performance tests {#iceberg-vs-mergetree}

The run time of the same perf test queries with the MergeTree tables of the test (the usual configuration) and with them created as Iceberg tables (`perf.py --table-engine`, see the "Iceberg configuration" section of `README.md`). A snapshot from the CI of https://github.com/ClickHouse/ClickHouse/pull/128261, commit e1b3517dafb90fd7867b9c37fc21256f94d64c79, run on 2026-10-31.

Source: `all-query-metrics.tsv` of the ten ARM shards of that run: `Performance Comparison (arm_release, master_head, N/6)` for MergeTree, `(arm_release, master_head, iceberg_local, N/2)` and `(arm_release, master_head, iceberg_s3, N/2)` for Iceberg. Dashboard: https://performance.ci.clickhouse.com/runs/pr-128261-e1b3517dafb90fd7867b9c37fc21256f94d64c79

How the numbers are computed:

* The time of a query is the median `client_time` of the tested server in its shard. All three configurations ran the same binary on the same runner type (`m8g.4xlarge`), but on different hosts.
* A pull request run measures up to 10 queries per test, so only the queries measured in both configurations are compared: 813 of the `IcebergLocal` queries and 817 of the `IcebergS3` ones.
* The slowdown of a test is the geometric mean over its queries of Iceberg time / MergeTree time; above 1x the Iceberg tables are slower. The time columns are the sums of the query medians, in seconds. "Worst query" is the largest per-query ratio, with its `query_index` (`perf.py --print-queries tests/performance/<test>.xml --queries-to-run <query_index>` prints it).
* `IcebergS3` writes to and reads from the job-local SeaweedFS, not a remote object store.

The ratios are not regressions. Many of these tests measure a MergeTree feature that an Iceberg table does not have (the primary key, skip and text indexes, projections, read in order, lightweight deletes), so a large ratio often means that the feature is not there. The ratios far below 1x are measured `OPTIMIZE` and `INSERT` statements that do much less work on an Iceberg table (no merge of parts, no index to build). The first rows are the places where an Iceberg table loses most against the same data in MergeTree; the comparison between two commits of the Iceberg configuration itself is in its own CI jobs and on the dashboard under `arm_iceberg_local` and `arm_iceberg_s3`.

## Summary {#summary}

| Backend | Tests | Queries | Median per-test slowdown | Geometric mean | Tests > 2x | Tests > 10x | Tests faster on Iceberg |
|---|---|---|---|---|---|---|---|
| `IcebergLocal` | 199 | 813 | 1.34x | 1.40x | 61 | 9 | 56 |
| `IcebergS3` | 196 | 817 | 1.60x | 1.69x | 77 | 11 | 42 |

## Per test, sorted by slowdown {#per-test}

| # | Test | Queries | MergeTree, s | `IcebergLocal`, s | `IcebergLocal` slowdown | Worst query (local) | `IcebergS3`, s | `IcebergS3` slowdown | Worst query (S3) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `ie_join_selectivity_key_choice` | 1 | 0.014 | 2.53 | 178x | 178x (#0) | 2.42 | 170x | 170x (#0) |
| 2 | `lightweight_delete` | 1 | 0.006 | 0.457 | 71x | 71x (#0) | 0.438 | 68x | 68x (#0) |
| 3 | `propagate_predicate_across_equi_join` | 5 | 0.667 | 3.10 | 42x | 136x (#0) | 3.52 | 48x | 157x (#0) |
| 4 | `text_index_multi_search_any` | 4 | 0.017 | 0.494 | 29x | 33x (#2) | 0.524 | 31x | 33x (#2) |
| 5 | `optimize_functions_to_subcolumns` | 6 | 0.043 | 1.43 | 27x | 60x (#2) | 1.53 | 30x | 65x (#2) |
| 6 | `join_seal_gated_reading` | 6 | 0.111 | 2.23 | 19x | 26x (#2) | 2.52 | 23x | 29x (#2) |
| 7 | `cast_nullable_read_in_order` | 2 | 0.012 | 0.180 | 15x | 15x (#0) | 0.194 | 16x | 16x (#0) |
| 8 | `codec_none` | 1 | 0.005 | 0.066 | 13x | 13x (#0) | 0.074 | 15x | 15x (#0) |
| 9 | `text_index_lazy_wide_granule` | 4 | 0.044 | 0.456 | 12x | 21x (#8) | 0.426 | 12x | 22x (#8) |
| 10 | `text_index_posting_list_codec` | 6 | 0.021 | 0.204 | 9.55x | 12x (#13) | 0.223 | 11x | 13x (#13) |
| 11 | `map_contains_value_subcolumn` | 3 | 1.71 | 7.12 | 9.35x | 23x (#1) | 7.64 | 10x | 25x (#1) |
| 12 | `map_keys_values_filter_only` | 4 | 0.866 | 5.82 | 9.36x | 22x (#0) | 6.07 | 9.77x | 24x (#0) |
| 13 | `map_contains_like_subcolumn` | 10 | 0.600 | 4.08 | 8.93x | 25x (#2) | 4.19 | 9.17x | 26x (#0) |
| 14 | `codecs_int_insert` | 6 | 0.320 | 1.15 | 4.02x | 9.19x (#5) | 2.85 | 9.04x | 14x (#5) |
| 15 | `materialized_view_deduplication` | 1 | 0.007 | 0.023 | 3.48x | 3.48x (#0) | 0.059 | 8.97x | 8.97x (#0) |
| 16 | `text_index_phrase_cursor` | 7 | 0.149 | 0.687 | 6.65x | 11x (#0) | 0.749 | 7.27x | 13x (#0) |
| 17 | `read_in_order_virtual_row_filtered` | 2 | 0.007 | 0.039 | 5.97x | 6.09x (#1) | 0.044 | 6.64x | 6.78x (#1) |
| 18 | `mmap_io` | 1 | 0.004 | 0.016 | 4.11x | 4.11x (#0) | 0.024 | 6.29x | 6.29x (#0) |
| 19 | `low_cardinality_many_marks_point_lookups` | 1 | 0.285 | 1.40 | 4.91x | 4.91x (#0) | 1.74 | 6.11x | 6.11x (#0) |
| 20 | `enum_in_set` | 1 | 0.273 | 1.41 | 5.18x | 5.18x (#0) | 1.66 | 6.08x | 6.08x (#0) |
| 21 | `parallel_mv` | 1 | 0.261 | 0.809 | 3.10x | 3.10x (#0) | 1.58 | 6.06x | 6.06x (#0) |
| 22 | `zstd_codec_context_reuse` | 3 | 1.42 | 1.64 | 0.22x | 2.60x (#0) | 4.22 | 6.00x | 7.60x (#1) |
| 23 | `constant_sort_key_insert` | 8 | 0.084 | 0.264 | 3.34x | 3.84x (#1) | 0.474 | 5.52x | 7.20x (#0) |
| 24 | `key_condition_single_column_key_point_lookup` | 2 | 0.006 | 0.022 | 3.89x | 4.16x (#1) | 0.030 | 5.25x | 5.34x (#1) |
| 25 | `text_index_prewhere_read_step_order` | 2 | 0.024 | 0.121 | 5.06x | 5.89x (#0) | 0.111 | 4.49x | 7.05x (#0) |
| 26 | `key_condition_multi_expression_key` | 2 | 0.006 | 0.025 | 3.95x | 4.43x (#0) | 0.031 | 4.90x | 5.43x (#0) |
| 27 | `codecs_float_insert` | 6 | 2.95 | 8.75 | 3.25x | 5.30x (#14) | 10.04 | 4.79x | 7.97x (#4) |
| 28 | `low_cardinality_nested_function_fast_paths` | 5 | 0.064 | 0.290 | 4.62x | 5.32x (#3) | 0.299 | 4.79x | 5.59x (#1) |
| 29 | `delta_decode` | 9 | 0.626 | 3.02 | 4.35x | 8.00x (#10) | 3.10 | 4.76x | 9.89x (#10) |
| 30 | `join_filter_pushdown_equivalent_sets` | 5 | 0.025 | 0.094 | 3.74x | 4.00x (#3) | 0.119 | 4.73x | 5.09x (#3) |
| 31 | `map_has_subcolumn` | 4 | 0.457 | 1.88 | 4.62x | 7.78x (#2) | 1.92 | 4.71x | 7.87x (#2) |
| 32 | `text_index_like_array` | 9 | 0.102 | 0.443 | 3.85x | 13x (#6) | 0.517 | 4.62x | 13x (#6) |
| 33 | `estimate_compression_ratio_t64` | 2 | 0.011 | 0.037 | 3.27x | 3.47x (#0) | 0.051 | 4.49x | 4.98x (#0) |
| 34 | `primary_key_exclusion_search_non_key_conditions` | 4 | 0.030 | 0.100 | 3.36x | 3.86x (#0) | 0.134 | 4.48x | 4.99x (#0) |
| 35 | `text_index_single_block_postings` | 6 | 0.040 | 0.135 | 3.51x | 4.35x (#0) | 0.170 | 4.42x | 5.73x (#0) |
| 36 | `codecs_multiple_select` | 1 | 0.897 | 2.87 | 3.21x | 3.21x (#0) | 3.84 | 4.29x | 4.29x (#0) |
| 37 | `full_sorting_merge_join` | 7 | 0.106 | 0.301 | 2.56x | 4.19x (#8) | 0.773 | 4.20x | 11x (#1) |
| 38 | `string_filters_during_scan` | 5 | 0.234 | 1.14 | 3.31x | 6.72x (#1) | 1.30 | 3.93x | 7.20x (#1) |
| 39 | `lazy_materialization_join` | 7 | 0.623 | 1.87 | 2.82x | 12x (#2) | 2.62 | 3.91x | 16x (#2) |
| 40 | `read_in_order_many_parts` | 8 | 1.28 | 3.14 | 3.21x | 22x (#0) | 3.55 | 3.77x | 24x (#0) |
| 41 | `prewhere_combine_filters` | 3 | 0.305 | 0.909 | 3.45x | 7.99x (#0) | 0.972 | 3.69x | 8.53x (#0) |
| 42 | `limit_by_in_pk_order` | 8 | 0.909 | 2.21 | 3.60x | 10x (#11) | 8.75 | 3.68x | 21x (#10) |
| 43 | `distinct_partitions` | 4 | 0.334 | 1.22 | 1.98x | 5.38x (#12) | 1.14 | 3.64x | 14x (#19) |
| 44 | `codecs_float_select` | 4 | 0.268 | 0.700 | 3.18x | 5.28x (#3) | 1.27 | 3.62x | 7.96x (#0) |
| 45 | `push_down_volume_reducing_functions` | 9 | 0.582 | 1.61 | 2.78x | 4.05x (#7) | 2.06 | 3.57x | 5.22x (#7) |
| 46 | `key_condition_multi_atom_large_in` | 1 | 0.012 | 0.034 | 2.92x | 2.92x (#0) | 0.039 | 3.31x | 3.31x (#0) |
| 47 | `text_index_like` | 8 | 0.087 | 0.330 | 2.54x | 24x (#1) | 0.385 | 3.24x | 29x (#1) |
| 48 | `map_bucketed_serialization` | 9 | 0.717 | 2.37 | 3.13x | 11x (#1) | 2.43 | 3.22x | 11x (#1) |
| 49 | `minmax_count_projection_plan_time` | 5 | 0.026 | 0.047 | 1.79x | 1.98x (#1) | 0.083 | 3.20x | 3.46x (#1) |
| 50 | `creating_set_partitions` | 5 | 0.835 | 0.890 | 1.69x | 2.88x (#6) | 1.54 | 3.18x | 13x (#10) |
| 51 | `merge_table_large_query` | 1 | 1.00 | 2.81 | 2.81x | 2.81x (#0) | 3.14 | 3.14x | 3.14x (#0) |
| 52 | `bloom_filter_select` | 2 | 0.012 | 0.032 | 2.64x | 2.65x (#0) | 0.036 | 3.01x | 3.03x (#1) |
| 53 | `limit_by_partitions` | 6 | 0.252 | 2.03 | 3.00x | 16x (#15) | 0.926 | 1.70x | 3.08x (#12) |
| 54 | `negative_limit_by_in_order` | 9 | 1.75 | 2.31 | 2.97x | 5.48x (#6) | 2.17 | 2.40x | 5.00x (#2) |
| 55 | `lz4` | 3 | 0.059 | 0.163 | 2.49x | 4.16x (#2) | 0.187 | 2.89x | 4.52x (#2) |
| 56 | `top_k_through_array_join` | 10 | 0.297 | 0.615 | 2.44x | 3.38x (#3) | 0.690 | 2.86x | 4.09x (#1) |
| 57 | `codecs_int_select` | 7 | 0.417 | 1.02 | 2.76x | 6.82x (#12) | 1.51 | 2.12x | 5.27x (#0) |
| 58 | `insert_statistics` | 7 | 1.24 | 1.14 | 1.32x | 5.03x (#6) | 2.31 | 2.71x | 11x (#6) |
| 59 | `add_distinct_to_in_clause` | 2 | 3.23 | 4.16 | 2.09x | 3.94x (#1) | 4.93 | 2.64x | 5.59x (#1) |
| 60 | `merge_tree_simple_select` | 1 | 0.001 | 0.002 | 1.85x | 1.85x (#0) | 0.003 | 2.62x | 2.62x (#0) |
| 61 | `optimize_time_filter_with_preimage` | 4 | 0.440 | 1.18 | 2.26x | 4.92x (#0) | 1.30 | 2.58x | 5.31x (#0) |
| 62 | `short_query_aggregation_overhead` | 1 | 0.117 | 0.204 | 1.75x | 1.75x (#0) | 0.294 | 2.52x | 2.52x (#0) |
| 63 | `enum_string_operation` | 6 | 2.41 | 5.18 | 2.15x | 3.29x (#14) | 5.54 | 2.51x | 3.26x (#14) |
| 64 | `join_convert_full_to_left_or_right` | 1 | 0.008 | 0.015 | 1.80x | 1.80x (#0) | 0.020 | 2.43x | 2.43x (#0) |
| 65 | `insert_sparse_column` | 1 | 0.133 | 0.288 | 2.17x | 2.17x (#0) | 0.321 | 2.41x | 2.41x (#0) |
| 66 | `merge_tree_insert` | 4 | 0.206 | 0.378 | 1.75x | 2.07x (#21) | 1.48 | 2.37x | 4.68x (#5) |
| 67 | `top_k_through_join` | 3 | 0.158 | 0.227 | 1.84x | 4.09x (#0) | 0.277 | 2.35x | 6.62x (#0) |
| 68 | `like_perfect_affix_rewrite` | 2 | 0.188 | 0.413 | 2.19x | 2.20x (#0) | 0.441 | 2.34x | 2.35x (#0) |
| 69 | `has_any_has_all_constant_numbers` | 5 | 0.219 | 0.328 | 1.32x | 1.63x (#3) | 0.565 | 2.31x | 4.47x (#10) |
| 70 | `low_cardinality_argument` | 4 | 0.031 | 0.049 | 1.59x | 1.75x (#1) | 0.069 | 2.28x | 2.50x (#1) |
| 71 | `has_all` | 6 | 0.225 | 0.436 | 2.14x | 4.98x (#8) | 0.738 | 2.26x | 5.79x (#8) |
| 72 | `sparse_column_index_sorted` | 2 | 0.919 | 2.19 | 2.23x | 2.55x (#1) | 2.19 | 2.24x | 2.55x (#1) |
| 73 | `key_condition_multi_atom_wide_or` | 1 | 0.022 | 0.042 | 1.87x | 1.87x (#0) | 0.050 | 2.23x | 2.23x (#0) |
| 74 | `has_any_constant_string_needles` | 8 | 2.39 | 3.22 | 2.09x | 14x (#2) | 3.30 | 2.19x | 15x (#2) |
| 75 | `low_cardinality_query` | 2 | 0.018 | 0.032 | 1.77x | 1.79x (#1) | 0.039 | 2.14x | 2.18x (#1) |
| 76 | `sorted_distinct` | 8 | 0.314 | 0.413 | 1.98x | 3.53x (#9) | 0.398 | 2.07x | 4.14x (#5) |
| 77 | `join_merge_filter_into_join` | 2 | 0.013 | 0.021 | 1.61x | 1.62x (#0) | 0.027 | 2.05x | 2.06x (#0) |
| 78 | `map_subcolumns_prewhere` | 9 | 2.21 | 4.61 | 1.97x | 3.26x (#4) | 4.75 | 2.02x | 3.45x (#4) |
| 79 | `set_index_insert_fixed_keys` | 2 | 0.470 | 0.586 | 1.25x | 1.36x (#1) | 0.917 | 1.95x | 2.07x (#1) |
| 80 | `group_by_sundy_li` | 3 | 0.093 | 0.141 | 1.62x | 1.82x (#0) | 0.162 | 1.94x | 2.43x (#0) |
| 81 | `window_partitions` | 6 | 0.855 | 1.55 | 1.78x | 2.02x (#5) | 1.80 | 1.89x | 2.23x (#10) |
| 82 | `aggregation_in_order_2` | 9 | 2.30 | 2.87 | 1.71x | 5.87x (#0) | 3.07 | 1.89x | 7.02x (#0) |
| 83 | `parallel_full_sorting_merge_join` | 8 | 1.31 | 2.04 | 1.71x | 6.31x (#4) | 2.18 | 1.89x | 6.44x (#4) |
| 84 | `text_index_low_cardinality_build` | 6 | 1.22 | 0.791 | 0.92x | 2.56x (#0) | 1.61 | 1.88x | 6.66x (#0) |
| 85 | `narrow_column_read_stream_limit` | 2 | 0.029 | 0.040 | 1.40x | 1.44x (#0) | 0.054 | 1.87x | 1.98x (#0) |
| 86 | `join_filter_pushdown` | 1 | 0.008 | 0.014 | 1.86x | 1.86x (#0) |  |  |  |
| 87 | `skip_index_order_after_partition_pruning` | 2 | 0.008 | 0.012 | 1.47x | 1.51x (#0) | 0.014 | 1.78x | 1.89x (#0) |
| 88 | `map_lowcardinality_key_lookup` | 5 | 0.757 | 1.24 | 1.71x | 2.43x (#4) | 1.28 | 1.77x | 2.49x (#4) |
| 89 | `set_index_check_in_range` | 1 | 0.234 | 0.406 | 1.74x | 1.74x (#0) | 0.406 | 1.74x | 1.74x (#0) |
| 90 | `count` | 3 | 0.080 | 0.092 | 1.36x | 1.91x (#0) | 0.103 | 1.72x | 3.18x (#0) |
| 91 | `join_convert_outer_to_inner_transitively` | 2 | 0.339 | 0.524 | 1.54x | 1.60x (#0) | 0.581 | 1.71x | 1.72x (#0) |
| 92 | `lot_of_subcolumns` | 1 | 0.038 | 0.066 | 1.70x | 1.70x (#0) | 0.065 | 1.69x | 1.69x (#0) |
| 93 | `group_by_multi_low_cardinality` | 4 | 0.435 | 0.503 | 1.55x | 4.47x (#2) | 0.534 | 1.70x | 5.28x (#2) |
| 94 | `trim_whitespace` | 6 | 0.768 | 1.07 | 1.42x | 1.64x (#3) | 1.23 | 1.67x | 2.08x (#3) |
| 95 | `text_index_analysis` | 8 | 0.122 | 0.099 | 1.48x | 2.18x (#5) | 0.112 | 1.66x | 2.42x (#5) |
| 96 | `distinct_low_cardinality_prelimit_distinct_in_order` | 6 | 0.307 | 0.229 | 1.59x | 2.92x (#4) | 0.228 | 1.62x | 3.10x (#4) |
| 97 | `date_time_64` | 6 | 1.35 | 1.74 | 1.57x | 3.38x (#6) | 1.23 | 1.61x | 2.73x (#12) |
| 98 | `text_index_count_stream_resolution` | 2 | 0.046 | 0.063 | 1.38x | 1.50x (#0) | 0.073 | 1.61x | 1.77x (#0) |
| 99 | `query_interpretation_wide_join` | 7 | 1.81 | 2.46 | 1.41x | 1.72x (#4) | 2.78 | 1.60x | 1.96x (#4) |
| 100 | `low_cardinality_insert` | 1 | 0.161 | 0.147 | 0.91x | 0.91x (#0) | 0.257 | 1.60x | 1.60x (#0) |
| 101 | `formats_columns_nullable` | 2 | 0.066 | 0.104 | 1.58x | 1.58x (#31) | 0.059 | 1.02x | 1.06x (#69) |
| 102 | `distinct_in_order_final_prefix` | 1 | 0.217 | 0.344 | 1.58x | 1.58x (#0) | 0.335 | 1.54x | 1.54x (#0) |
| 103 | `minmax_index_insert` | 1 | 0.344 | 0.325 | 0.94x | 0.94x (#0) | 0.538 | 1.56x | 1.56x (#0) |
| 104 | `set_index_analysis` | 1 | 0.007 | 0.009 | 1.13x | 1.13x (#0) | 0.012 | 1.56x | 1.56x (#0) |
| 105 | `distinct_low_cardinality` | 2 | 0.034 | 0.039 | 1.40x | 2.41x (#1) | 0.043 | 1.55x | 2.86x (#1) |
| 106 | `bloom_filter_insert_low_cardinality` | 2 | 1.10 | 1.05 | 1.28x | 2.42x (#0) | 1.27 | 1.55x | 2.96x (#0) |
| 107 | `join_lazy_replication_sort` | 3 | 0.792 | 1.13 | 1.44x | 1.87x (#2) | 1.20 | 1.53x | 2.04x (#2) |
| 108 | `bloom_filter_insert` | 1 | 0.193 | 0.162 | 0.84x | 0.84x (#0) | 0.284 | 1.47x | 1.47x (#0) |
| 109 | `partial_merge_join` | 4 | 0.018 | 0.016 | 0.90x | 0.90x (#1) | 0.153 | 1.43x | 1.88x (#0) |
| 110 | `array_join_filter_fusion` | 7 | 0.722 | 0.996 | 1.42x | 1.93x (#6) | 0.982 | 1.41x | 1.97x (#6) |
| 111 | `join_convert_to_fixed_hash_table` | 3 | 0.035 | 0.033 | 0.96x | 0.96x (#7) | 0.238 | 1.41x | 1.44x (#18) |
| 112 | `preliminary_distinct_abandoning` | 2 | 0.368 | 0.423 | 1.26x | 1.62x (#0) | 0.473 | 1.41x | 1.77x (#0) |
| 113 | `array_join_function_lowering` | 3 | 0.280 | 0.280 | 1.21x | 1.95x (#2) | 0.289 | 1.40x | 2.76x (#2) |
| 114 | `IPv4` | 4 | 0.119 | 0.159 | 1.34x | 1.39x (#0) | 0.163 | 1.38x | 1.45x (#0) |
| 115 | `group_by_multiple_strings` | 5 | 1.57 | 1.32 | 0.93x | 1.32x (#16) | 0.672 | 1.37x | 1.91x (#23) |
| 116 | `group_by_low_cardinality_short_strings` | 4 | 0.171 | 0.228 | 1.34x | 1.38x (#1) | 0.234 | 1.37x | 1.50x (#1) |
| 117 | `aggregation_single_level_result_fanout` | 3 | 0.526 | 0.703 | 1.34x | 1.41x (#0) | 0.718 | 1.37x | 1.46x (#0) |
| 118 | `parameterized_view_analysis` | 2 | 0.375 | 0.449 | 1.26x | 1.39x (#1) | 0.479 | 1.36x | 1.55x (#1) |
| 119 | `aggregation_low_cardinality` | 2 | 0.069 | 0.082 | 1.20x | 1.23x (#0) | 0.093 | 1.36x | 1.41x (#0) |
| 120 | `inserts_arrays_lowcardinality` | 2 | 0.642 | 0.337 | 0.64x | 1.25x (#0) | 0.711 | 1.35x | 2.63x (#0) |
| 121 | `all_join_opt` | 7 | 0.266 | 0.319 | 1.19x | 1.48x (#2) | 0.356 | 1.34x | 1.63x (#2) |
| 122 | `timeseries_to_grid_bucketing` | 7 | 1.52 | 1.81 | 1.27x | 1.75x (#0) | 1.82 | 1.31x | 1.69x (#0) |
| 123 | `simple_join_query` | 4 | 0.171 | 0.192 | 1.12x | 1.14x (#2) | 0.221 | 1.29x | 1.30x (#3) |
| 124 | `aggregation_by_partitions` | 4 | 0.016 | 0.016 | 1.01x | 1.01x (#13) | 0.099 | 1.29x | 2.14x (#15) |
| 125 | `dict_join` | 2 | 0.075 | 0.091 | 1.28x | 1.42x (#0) | 0.091 | 1.29x | 1.46x (#0) |
| 126 | `sparse_column` | 1 | 0.268 | 0.341 | 1.27x | 1.27x (#61) |  |  |  |
| 127 | `point_in_polygon_index` | 1 | 0.005 | 0.006 | 1.25x | 1.25x (#0) |  |  |  |
| 128 | `text_index_pattern_bypass_cache` | 2 | 0.021 | 0.022 | 1.02x | 1.09x (#0) | 0.026 | 1.24x | 1.32x (#0) |
| 129 | `countIf` | 4 | 0.148 | 0.158 | 1.17x | 1.91x (#3) | 0.162 | 1.22x | 2.20x (#3) |
| 130 | `set_in_clustered_keys` | 4 | 1.91 | 1.99 | 1.13x | 1.39x (#2) | 2.02 | 1.21x | 1.66x (#2) |
| 131 | `join_squashing_blocks` | 3 | 0.283 | 0.324 | 1.14x | 1.26x (#2) | 0.343 | 1.21x | 1.31x (#2) |
| 132 | `sparse_column_index_runs` | 6 | 0.525 | 0.595 | 1.21x | 1.56x (#5) | 0.591 | 1.20x | 1.57x (#5) |
| 133 | `limit_by_with_order` | 5 | 1.78 | 1.92 | 1.20x | 2.21x (#1) | 0.075 | 0.99x | 0.99x (#17) |
| 134 | `uniq_without_key` | 5 | 0.563 | 0.444 | 0.79x | 1.05x (#15) | 0.593 | 1.19x | 1.34x (#2) |
| 135 | `window_functions_sliding_frames` | 8 | 6.29 | 7.61 | 1.14x | 1.87x (#7) | 8.62 | 1.16x | 1.88x (#7) |
| 136 | `right_full_hash_join` | 8 | 1.48 | 1.67 | 1.13x | 1.17x (#6) | 1.67 | 1.15x | 1.25x (#1) |
| 137 | `uniq_with_key` | 8 | 2.75 | 3.06 | 1.12x | 1.97x (#6) | 2.89 | 1.06x | 1.37x (#4) |
| 138 | `hierarchical_dictionaries` | 10 | 3.35 | 3.49 | 1.05x | 1.12x (#0) | 3.68 | 1.12x | 1.27x (#0) |
| 139 | `wide_table_nested_select` | 3 | 0.517 | 0.526 | 1.03x | 1.07x (#0) | 0.565 | 1.11x | 1.17x (#0) |
| 140 | `sort_multi_column_mixed` | 6 | 1.84 | 1.86 | 1.05x | 1.18x (#3) | 1.97 | 1.11x | 1.21x (#1) |
| 141 | `window_functions_ranking` | 10 | 7.47 | 8.21 | 1.10x | 1.12x (#6) | 9.12 | 1.09x | 1.10x (#1) |
| 142 | `sort_string_full` | 4 | 2.58 | 2.49 | 1.00x | 1.10x (#1) | 2.75 | 1.10x | 1.18x (#3) |
| 143 | `joined_block_split_single_row` | 3 | 0.146 | 0.148 | 1.01x | 1.02x (#1) | 0.160 | 1.10x | 1.14x (#1) |
| 144 | `right_anti_join_with_residual_filter` | 4 | 0.898 | 0.856 | 0.96x | 1.03x (#3) | 0.977 | 1.09x | 1.17x (#3) |
| 145 | `array_join` | 8 | 2.45 | 2.23 | 0.94x | 1.07x (#7) | 2.01 | 1.08x | 1.36x (#10) |
| 146 | `uniq_with_key_dist` | 8 | 1.22 | 1.31 | 1.08x | 1.18x (#6) | 1.22 | 1.01x | 1.07x (#1) |
| 147 | `dict_get_keys_vector_path` | 2 | 1.94 | 2.11 | 1.08x | 1.12x (#1) | 2.13 | 1.07x | 1.16x (#1) |
| 148 | `sort_wide_table` | 4 | 1.76 | 1.64 | 0.98x | 1.13x (#3) | 1.79 | 1.07x | 1.21x (#3) |
| 149 | `window_functions_qualify` | 5 | 3.45 | 3.63 | 1.05x | 1.10x (#4) | 3.70 | 1.07x | 1.12x (#4) |
| 150 | `window_functions_many_small_partitions` | 7 | 5.87 | 6.27 | 1.07x | 1.11x (#3) | 6.26 | 1.07x | 1.10x (#3) |
| 151 | `dict_get_keys_const_path` | 4 | 0.549 | 0.577 | 1.05x | 1.08x (#1) | 0.567 | 1.03x | 1.09x (#1) |
| 152 | `merge_tree_many_partitions` | 1 | 0.003 | 0.002 | 0.75x | 0.75x (#0) | 0.003 | 1.04x | 1.04x (#0) |
| 153 | `aggregation_two_level_bucket_merge` | 4 | 3.52 | 3.49 | 1.03x | 1.29x (#3) | 3.31 | 0.98x | 1.25x (#3) |
| 154 | `subqueries` | 2 | 0.408 | 0.411 | 1.01x | 1.01x (#0) | 0.418 | 1.02x | 1.03x (#1) |
| 155 | `jit_sort` | 7 | 6.69 | 6.83 | 1.02x | 1.04x (#13) | 7.16 | 1.01x | 1.03x (#12) |
| 156 | `sort_nearly_sorted` | 2 | 0.534 | 0.492 | 1.00x | 1.15x (#1) | 0.491 | 1.02x | 1.20x (#1) |
| 157 | `window_functions_downstream_multithreading` | 6 | 3.68 | 3.49 | 0.94x | 1.11x (#5) | 3.66 | 1.01x | 1.23x (#5) |
| 158 | `sort_low_cardinality` | 6 | 1.50 | 1.34 | 0.98x | 2.19x (#0) | 1.35 | 1.01x | 2.19x (#0) |
| 159 | `left_array_join_long_arrays` | 6 | 1.81 | 1.69 | 0.97x | 1.17x (#5) | 1.75 | 1.01x | 1.16x (#5) |
| 160 | `has_any_has_all_nullable` | 4 | 0.543 | 0.454 | 1.00x | 2.55x (#8) | 0.518 | 0.42x | 1.53x (#12) |
| 161 | `IPv6` | 2 | 0.305 | 0.292 | 0.96x | 0.96x (#1) | 0.297 | 0.98x | 0.99x (#0) |
| 162 | `uniq_without_key_dist` | 5 | 1.22 | 1.15 | 0.91x | 1.05x (#4) | 1.19 | 0.96x | 1.04x (#4) |
| 163 | `groupby_onekey_nullable` | 9 | 1.75 | 1.32 | 0.73x | 0.94x (#6) | 1.71 | 0.96x | 1.20x (#6) |
| 164 | `aggregate_partitions_independently_default` | 4 | 0.419 | 0.368 | 0.92x | 1.01x (#2) | 0.383 | 0.95x | 1.04x (#0) |
| 165 | `correlated_exists_many_keys` | 1 | 0.532 | 0.501 | 0.94x | 0.94x (#0) | 0.484 | 0.91x | 0.91x (#0) |
| 166 | `group_by_fixed_keys` | 6 | 0.696 | 0.645 | 0.94x | 1.38x (#11) | 0.717 | 0.82x | 1.42x (#11) |
| 167 | `storage_join_direct_join` | 1 | 0.246 | 0.209 | 0.85x | 0.85x (#0) | 0.220 | 0.90x | 0.90x (#0) |
| 168 | `hash_join_with_row_store` | 6 | 7.92 | 7.29 | 0.87x | 1.19x (#17) | 6.79 | 0.89x | 1.13x (#16) |
| 169 | `point_in_polygon_3d_huge_multipolygon` | 1 | 0.035 | 0.029 | 0.84x | 0.84x (#0) | 0.030 | 0.86x | 0.86x (#0) |
| 170 | `uniqExact_with_key_parallel_merging` | 4 | 2.36 | 1.83 | 0.81x | 0.88x (#3) | 1.85 | 0.82x | 0.93x (#3) |
| 171 | `bloom_filter_zstd` | 2 | 0.034 | 0.027 | 0.57x | 1.12x (#1) | 0.040 | 0.81x | 1.71x (#1) |
| 172 | `set_disable_skip_index` | 1 | 0.030 | 0.018 | 0.61x | 0.61x (#0) | 0.023 | 0.77x | 0.77x (#0) |
| 173 | `sparse_column_filter` | 9 | 5.64 | 1.64 | 0.61x | 2.11x (#5) | 1.96 | 0.73x | 2.82x (#5) |
| 174 | `query_condition_cache_topk` | 2 | 0.087 | 0.010 | 0.37x | 1.71x (#0) | 0.021 | 0.73x | 3.33x (#0) |
| 175 | `text_index_build` | 9 | 3.53 | 0.916 | 0.39x | 0.77x (#1) | 1.66 | 0.72x | 1.46x (#1) |
| 176 | `timeseries_to_grid_scalar_add` | 3 | 0.472 | 0.275 | 0.67x | 0.89x (#2) | 0.294 | 0.71x | 0.89x (#2) |
| 177 | `prefetch_in_aggregation` | 7 | 3.00 | 1.88 | 0.70x | 1.07x (#8) | 1.19 | 0.30x | 1.02x (#9) |
| 178 | `projection_index_in_read_pools` | 4 | 0.130 | 0.065 | 0.53x | 0.80x (#1) | 0.086 | 0.69x | 1.04x (#1) |
| 179 | `aggregation_in_order_partial_key` | 10 | 1.56 | 0.883 | 0.65x | 1.40x (#5) | 0.900 | 0.68x | 1.56x (#5) |
| 180 | `permuted_columns_cache` | 1 | 0.639 | 0.245 | 0.38x | 0.38x (#0) | 0.433 | 0.68x | 0.68x (#0) |
| 181 | `subcolumn_deserialization_tuple_map` | 10 | 4.75 | 2.18 | 0.48x | 0.78x (#2) | 2.34 | 0.51x | 0.85x (#2) |
| 182 | `window_function_with_order` | 4 | 1.29 | 0.442 | 0.49x | 1.20x (#1) | 0.457 | 0.51x | 1.24x (#1) |
| 183 | `skip_indexes_in_read_pools` | 4 | 0.062 | 0.015 | 0.24x | 0.24x (#2) | 0.095 | 0.47x | 0.57x (#1) |
| 184 | `text_index_postprocessor_filter` | 4 | 4.47 | 1.35 | 0.30x | 0.33x (#2) | 2.09 | 0.47x | 0.51x (#2) |
| 185 | `text_index_like_large_dictionary` | 7 | 3.58 | 1.83 | 0.35x | 1.06x (#5) | 2.01 | 0.42x | 1.16x (#5) |
| 186 | `statistics_plan_time_uniq` | 2 | 0.431 | 0.166 | 0.38x | 0.40x (#1) | 0.168 | 0.38x | 0.40x (#1) |
| 187 | `memory_bound_merging` | 1 | 0.415 | 0.146 | 0.35x | 0.35x (#0) | 0.158 | 0.38x | 0.38x (#0) |
| 188 | `insert_statistics_countmin` | 4 | 2.12 | 0.211 | 0.093x | 0.34x (#3) | 0.778 | 0.36x | 1.23x (#3) |
| 189 | `top_k_threshold_tracker` | 2 | 0.256 | 0.052 | 0.20x | 0.21x (#0) | 0.075 | 0.29x | 0.30x (#0) |
| 190 | `sparse_grams_index_build` | 5 | 9.25 | 2.40 | 0.20x | 1.01x (#4) | 2.76 | 0.29x | 0.98x (#3) |
| 191 | `parallel_index` | 3 | 0.666 | 0.043 | 0.17x | 1.70x (#2) | 0.048 | 0.19x | 1.98x (#2) |
| 192 | `text_index_build_tokenizers` | 3 | 3.51 | 0.290 | 0.083x | 0.087x (#1) | 0.551 | 0.16x | 0.18x (#1) |
| 193 | `array_index_low_cardinality_strings` | 10 | 6.05 | 0.781 | 0.12x | 0.22x (#0) | 0.830 | 0.13x | 0.24x (#0) |
| 194 | `long_primary_key` | 3 | 0.453 | 0.038 | 0.084x | 0.086x (#1) | 0.050 | 0.11x | 0.11x (#1) |
| 195 | `partition_pruning_trace_formatting` | 1 | 0.091 | 0.004 | 0.047x | 0.047x (#0) | 0.005 | 0.060x | 0.060x (#0) |
| 196 | `merge_tree_many_partitions_2` | 1 | 0.188 | 0.004 | 0.022x | 0.022x (#0) | 0.008 | 0.042x | 0.042x (#0) |
| 197 | `insert_sequential_and_background_merges` | 1 | 2.32 | 0.065 | 0.028x | 0.028x (#0) | 0.078 | 0.034x | 0.034x (#0) |
| 198 | `merge_tree_small_blocks` | 5 | 3.41 | 0.061 | 0.017x | 0.024x (#0) | 0.112 | 0.032x | 0.043x (#0) |
| 199 | `adaptive_codec_merge` | 3 | 2.71 | 0.008 | 0.003x | 0.003x (#0) | 0.013 | 0.005x | 0.006x (#0) |
