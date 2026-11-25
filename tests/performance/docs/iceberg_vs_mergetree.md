# Iceberg vs MergeTree in the performance tests {#iceberg-vs-mergetree}

The run time of the same perf test queries with the MergeTree tables of the test (the usual configuration) and with them created as Iceberg tables (`perf.py --table-engine`, see the "Iceberg configuration" section of `README.md`). A snapshot from the CI of https://github.com/ClickHouse/ClickHouse/pull/128261, commit 215bbd6cc18a4f5021ba11af645e6010c8c519c7, run on 2026-11-02.

Source: `all-query-metrics.tsv` of the twelve ARM shards of that run: `Performance Comparison (arm_release, master_head, N/6)` for MergeTree, `(arm_release, master_head, iceberg_local, N/3)` and `(arm_release, master_head, iceberg_s3, N/3)` for Iceberg. Dashboard: https://performance.ci.clickhouse.com/runs/pr-128261-215bbd6cc18a4f5021ba11af645e6010c8c519c7

How the numbers are computed:

* A test marked (`Memory`) has `Memory` tables in the usual configuration: its MergeTree column reads RAM, not storage, so its ratio is mostly the cost of reading and decoding Parquet.
* The time of a query is the median `client_time` of the tested server in its shard. All three configurations ran the same binary on the same runner type (`m8g.4xlarge`), but on different hosts.
* A pull request run measures up to 10 queries per test, so only the queries measured in both configurations are compared: 1187 of the `IcebergLocal` queries and 1200 of the `IcebergS3` ones.
* The slowdown of a test is the geometric mean over its queries of Iceberg time / MergeTree time; above 1x the Iceberg tables are slower. The time columns are the sums of the query medians, in seconds. "Worst query" is the largest per-query ratio, with its `query_index` (`perf.py --print-queries tests/performance/<test>.xml --queries-to-run <query_index>` prints it).
* `IcebergS3` writes to and reads from the job-local SeaweedFS, not a remote object store.

The ratios are not regressions. Many of these tests measure a MergeTree feature that an Iceberg table does not have (the primary key, skip and text indexes, projections, read in order, lightweight deletes), so a large ratio often means that the feature is not there. The ratios far below 1x are measured `OPTIMIZE` and `INSERT` statements that do much less work on an Iceberg table (no merge of parts, no index to build). The first rows are the places where an Iceberg table loses most against the same data in MergeTree; the comparison between two commits of the Iceberg configuration itself is in its own CI jobs and on the dashboard under `arm_iceberg_local` and `arm_iceberg_s3`.

## Summary {#summary}

| Backend | Tests | Queries | Median per-test slowdown | Geometric mean | Tests > 2x | Tests > 10x | Tests faster on Iceberg |
|---|---|---|---|---|---|---|---|
| `IcebergLocal` | 282 | 1187 | 1.43x | 1.59x | 98 | 12 | 59 |
| `IcebergS3` | 282 | 1200 | 1.70x | 1.87x | 121 | 19 | 49 |

## Per test, sorted by slowdown {#per-test}

| # | Test | Queries | MergeTree, s | `IcebergLocal`, s | `IcebergLocal` slowdown | Worst query (local) | `IcebergS3`, s | `IcebergS3` slowdown | Worst query (S3) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `ie_join_selectivity_key_choice` | 1 | 0.014 | 2.42 | 175x | 175x (#0) | 2.39 | 173x | 173x (#0) |
| 2 | `lightweight_delete` | 1 | 0.006 | 0.480 | 76x | 76x (#0) | 0.482 | 77x | 77x (#0) |
| 3 | `propagate_predicate_across_equi_join` | 5 | 0.653 | 3.18 | 49x | 167x (#0) | 3.49 | 53x | 183x (#0) |
| 4 | `point_in_polygon` (`Memory`) | 1 | 0.089 | 3.22 | 36x | 36x (#0) | 3.49 | 39x | 39x (#0) |
| 5 | `text_index_multi_search_any` | 4 | 0.014 | 0.508 | 36x | 42x (#2) | 0.527 | 38x | 42x (#2) |
| 6 | `optimize_functions_to_subcolumns` | 6 | 0.042 | 1.38 | 27x | 61x (#2) | 1.43 | 28x | 61x (#2) |
| 7 | `join_seal_gated_reading` | 6 | 0.110 | 2.21 | 20x | 26x (#2) | 2.44 | 22x | 29x (#2) |
| 8 | `jit_large_requests` (`Memory`) | 2 | 0.207 | 3.71 | 18x | 22x (#1) | 4.02 | 20x | 24x (#1) |
| 9 | `array_index_simd_numeric` (`Memory`) | 3 | 0.071 | 0.660 | 18x | 78x (#1) | 0.658 | 18x | 79x (#1) |
| 10 | `apply_patch_parts_merge` | 4 | 0.613 | 9.59 | 16x | 20x (#0) | 9.90 | 16x | 21x (#0) |
| 11 | `codec_none` | 1 | 0.005 | 0.068 | 14x | 14x (#0) | 0.078 | 16x | 16x (#0) |
| 12 | `cast_nullable_read_in_order` | 2 | 0.012 | 0.175 | 14x | 14x (#1) | 0.191 | 16x | 16x (#1) |
| 13 | `opt_max_min` (`Memory`) | 8 | 0.072 | 0.702 | 9.61x | 16x (#5) | 1.09 | 14x | 22x (#5) |
| 14 | `text_index_lazy_wide_granule` | 4 | 0.071 | 0.591 | 8.34x | 10x (#19) | 0.416 | 12x | 24x (#12) |
| 15 | `text_index_posting_list_codec` | 7 | 0.025 | 0.217 | 8.81x | 9.81x (#8) | 0.192 | 11x | 12x (#8) |
| 16 | `materialized_view_deduplication` | 1 | 0.006 | 0.022 | 3.37x | 3.37x (#0) | 0.067 | 10x | 10x (#0) |
| 17 | `array_first_last` (`Memory`) | 9 | 0.117 | 0.960 | 9.61x | 31x (#3) | 1.38 | 10x | 31x (#3) |
| 18 | `map_contains_value_subcolumn` | 3 | 1.74 | 7.07 | 9.35x | 23x (#1) | 7.66 | 10x | 25x (#1) |
| 19 | `map_keys_values_filter_only` | 4 | 0.844 | 5.82 | 9.52x | 22x (#0) | 6.17 | 10x | 24x (#0) |
| 20 | `map_contains_like_subcolumn` | 10 | 0.609 | 4.00 | 8.75x | 25x (#6) | 4.36 | 9.54x | 27x (#8) |
| 21 | `codecs_int_insert` | 8 | 0.257 | 0.944 | 3.77x | 5.15x (#6) | 3.83 | 8.86x | 11x (#4) |
| 22 | `subsecond_extractors` (`Memory`) | 3 | 0.506 | 3.59 | 7.09x | 7.30x (#1) | 4.20 | 8.30x | 8.55x (#1) |
| 23 | `memory_compressed_subcolumns` (`Memory`) | 3 | 0.042 | 0.317 | 7.54x | 7.71x (#0) | 0.348 | 8.28x | 8.36x (#0) |
| 24 | `array_join_nested_columns` (`Memory`) | 2 | 0.017 | 0.111 | 7.76x | 15x (#0) | 0.116 | 8.15x | 15x (#0) |
| 25 | `array_distance` (`Memory`) | 3 | 0.227 | 1.51 | 7.93x | 9.21x (#4) | 1.55 | 6.52x | 9.48x (#4) |
| 26 | `least_greatest_nullable_variadic` (`Memory`) | 7 | 0.042 | 0.273 | 6.45x | 7.40x (#0) | 0.326 | 7.74x | 8.96x (#0) |
| 27 | `text_index_phrase_cursor` | 7 | 0.144 | 0.682 | 6.91x | 13x (#0) | 0.724 | 7.35x | 13x (#0) |
| 28 | `codecs_float_insert` | 5 | 2.38 | 5.54 | 2.58x | 4.46x (#14) | 9.87 | 7.30x | 12x (#14) |
| 29 | `read_in_order_virtual_row_filtered` | 2 | 0.007 | 0.038 | 5.73x | 5.76x (#1) | 0.047 | 7.17x | 7.18x (#0) |
| 30 | `join_runtime_filter` | 5 | 0.270 | 1.33 | 5.12x | 5.84x (#0) | 0.963 | 6.59x | 15x (#1) |
| 31 | `array_distance_simd_speedup` (`Memory`) | 8 | 0.519 | 2.64 | 6.12x | 12x (#2) | 2.61 | 5.55x | 9.52x (#5) |
| 32 | `mmap_io` | 1 | 0.004 | 0.016 | 3.88x | 3.88x (#0) | 0.025 | 6.07x | 6.07x (#0) |
| 33 | `enum_in_set` | 1 | 0.276 | 1.41 | 5.12x | 5.12x (#0) | 1.67 | 6.07x | 6.07x (#0) |
| 34 | `low_cardinality_many_marks_point_lookups` | 1 | 0.293 | 1.40 | 4.77x | 4.77x (#0) | 1.77 | 6.04x | 6.04x (#0) |
| 35 | `has_all` | 5 | 1.07 | 0.659 | 2.06x | 17x (#17) | 0.661 | 5.89x | 34x (#17) |
| 36 | `string_filters_during_scan` | 5 | 0.190 | 1.12 | 5.82x | 7.08x (#3) | 1.20 | 3.81x | 6.50x (#3) |
| 37 | `isValidASCII` (`Memory`) | 7 | 0.043 | 0.202 | 4.82x | 16x (#6) | 0.217 | 5.79x | 16x (#6) |
| 38 | `zstd_codec_context_reuse` | 3 | 1.43 | 1.52 | 0.21x | 2.48x (#0) | 4.09 | 5.79x | 7.38x (#1) |
| 39 | `constant_sort_key_insert` | 9 | 0.107 | 0.310 | 3.14x | 3.85x (#1) | 0.573 | 5.70x | 7.17x (#6) |
| 40 | `hashjoin_low_cardinality_key` (`Memory`) | 2 | 0.039 | 0.189 | 4.79x | 4.84x (#1) | 0.223 | 5.65x | 5.70x (#0) |
| 41 | `parallel_mv` | 1 | 0.259 | 0.829 | 3.21x | 3.21x (#0) | 1.43 | 5.54x | 5.54x (#0) |
| 42 | `text_index_prewhere_read_step_order` | 2 | 0.010 | 0.056 | 5.52x | 5.52x (#0) | 0.120 | 4.98x | 6.69x (#0) |
| 43 | `key_condition_multi_expression_key` | 2 | 0.005 | 0.024 | 4.42x | 4.92x (#0) | 0.029 | 5.39x | 6.08x (#0) |
| 44 | `join_filter_pushdown_equivalent_sets` | 5 | 0.020 | 0.089 | 4.41x | 4.68x (#3) | 0.108 | 5.31x | 5.50x (#3) |
| 45 | `delta_decode` | 8 | 0.476 | 2.44 | 4.39x | 8.31x (#2) | 3.02 | 5.20x | 9.38x (#6) |
| 46 | `sort_patterns` (`Memory`) | 8 | 2.90 | 11.97 | 4.38x | 7.17x (#0) | 14.22 | 5.20x | 8.43x (#0) |
| 47 | `low_cardinality_nested_function_fast_paths` | 5 | 0.062 | 0.288 | 4.68x | 5.42x (#3) | 0.298 | 4.88x | 5.55x (#3) |
| 48 | `map_has_subcolumn` | 4 | 0.450 | 1.83 | 4.53x | 7.40x (#2) | 1.90 | 4.70x | 7.70x (#2) |
| 49 | `limit_by_in_pk_order` | 8 | 0.972 | 2.90 | 3.57x | 9.02x (#8) | 9.63 | 4.70x | 20x (#10) |
| 50 | `estimate_compression_ratio_t64` | 2 | 0.010 | 0.037 | 3.54x | 3.62x (#0) | 0.049 | 4.68x | 5.06x (#0) |
| 51 | `key_condition_single_column_key_point_lookup` | 2 | 0.006 | 0.023 | 3.73x | 4.09x (#1) | 0.028 | 4.67x | 5.00x (#1) |
| 52 | `text_index_like_array` | 9 | 0.106 | 0.424 | 3.52x | 12x (#6) | 0.531 | 4.59x | 13x (#6) |
| 53 | `utc_timestamp_transform` | 4 | 0.175 | 0.646 | 3.86x | 5.20x (#1) | 0.758 | 4.53x | 6.11x (#1) |
| 54 | `text_index_single_block_postings` | 6 | 0.040 | 0.138 | 3.59x | 4.22x (#0) | 0.167 | 4.39x | 5.54x (#0) |
| 55 | `limit_by_partitions` | 6 | 1.00 | 0.922 | 1.38x | 2.21x (#21) | 2.64 | 4.32x | 41x (#13) |
| 56 | `lazy_materialization_join` | 7 | 0.613 | 1.83 | 2.85x | 11x (#2) | 2.82 | 4.27x | 17x (#2) |
| 57 | `prewhere_combine_filters` | 3 | 0.274 | 0.893 | 3.89x | 9.99x (#0) | 0.972 | 4.24x | 11x (#0) |
| 58 | `string_set` | 7 | 1.45 | 5.81 | 4.23x | 5.93x (#3) | 5.07 | 3.66x | 5.27x (#3) |
| 59 | `distinct_in_order` | 8 | 0.478 | 3.62 | 3.90x | 23x (#2) | 3.64 | 4.22x | 23x (#2) |
| 60 | `primary_key_exclusion_search_non_key_conditions` | 4 | 0.022 | 0.079 | 3.55x | 4.20x (#0) | 0.124 | 4.16x | 4.68x (#0) |
| 61 | `codecs_multiple_select` | 1 | 0.893 | 2.72 | 3.04x | 3.04x (#0) | 3.67 | 4.11x | 4.11x (#0) |
| 62 | `minmax_count_projection_plan_time` | 5 | 0.023 | 0.045 | 1.95x | 2.07x (#1) | 0.093 | 4.04x | 4.37x (#1) |
| 63 | `array_uniq_decimal` (`Memory`) | 10 | 0.077 | 0.265 | 3.64x | 5.17x (#8) | 0.286 | 4.01x | 5.57x (#8) |
| 64 | `full_sorting_merge_join` | 8 | 0.190 | 1.09 | 3.78x | 12x (#13) | 0.891 | 3.93x | 10x (#1) |
| 65 | `clamp_numeric` (`Memory`) | 3 | 0.019 | 0.063 | 3.34x | 4.04x (#0) | 0.073 | 3.89x | 4.65x (#0) |
| 66 | `skip_index_planning_with_patch_parts` | 2 | 0.037 | 0.080 | 2.15x | 2.19x (#1) | 0.143 | 3.85x | 3.92x (#1) |
| 67 | `non_function_col_short_circuit` (`Memory`) | 4 | 0.021 | 0.066 | 3.13x | 3.33x (#0) | 0.076 | 3.66x | 3.91x (#1) |
| 68 | `merge_tree_simple_select` | 1 | 0.001 | 0.002 | 1.54x | 1.54x (#0) | 0.005 | 3.62x | 3.62x (#0) |
| 69 | `push_down_volume_reducing_functions` | 9 | 0.562 | 1.59 | 2.85x | 4.25x (#7) | 2.01 | 3.60x | 5.32x (#7) |
| 70 | `merge_table_large_query` | 1 | 0.962 | 2.69 | 2.80x | 2.80x (#0) | 3.42 | 3.55x | 3.55x (#0) |
| 71 | `read_in_order_many_parts` | 8 | 1.28 | 3.11 | 3.14x | 21x (#0) | 3.36 | 3.53x | 22x (#0) |
| 72 | `lower_upper_utf8_mixed_ascii` (`Memory`) | 5 | 0.024 | 0.068 | 2.84x | 3.00x (#4) | 0.081 | 3.52x | 5.17x (#12) |
| 73 | `aggregate_function_for_each` (`Memory`) | 7 | 0.249 | 0.461 | 3.24x | 6.03x (#0) | 0.473 | 3.39x | 6.18x (#0) |
| 74 | `decimal_aggregates` (`Memory`) | 4 | 0.562 | 2.38 | 3.37x | 18x (#1) | 1.80 | 2.52x | 24x (#1) |
| 75 | `key_condition_multi_atom_large_in` | 1 | 0.012 | 0.035 | 2.96x | 2.96x (#0) | 0.040 | 3.36x | 3.36x (#0) |
| 76 | `map_bucketed_serialization` | 9 | 0.688 | 2.39 | 3.32x | 12x (#1) | 2.37 | 3.32x | 12x (#1) |
| 77 | `single_fixed_string_groupby` | 2 | 0.023 | 0.070 | 3.11x | 3.43x (#1) | 0.075 | 3.32x | 3.64x (#1) |
| 78 | `short_query_aggregation_overhead` | 1 | 0.117 | 0.199 | 1.70x | 1.70x (#0) | 0.373 | 3.20x | 3.20x (#0) |
| 79 | `low_cardinality_bulk_insert` (`Memory`) | 1 | 0.104 | 0.322 | 3.09x | 3.09x (#0) | 0.328 | 3.14x | 3.14x (#0) |
| 80 | `text_index_like` | 8 | 0.087 | 0.328 | 2.55x | 20x (#1) | 0.364 | 3.13x | 22x (#1) |
| 81 | `joins_in_memory` (`Memory`) | 4 | 0.207 | 0.238 | 1.64x | 2.43x (#2) | 0.022 | 3.06x | 3.17x (#2) |
| 82 | `bloom_filter_select` | 2 | 0.012 | 0.030 | 2.49x | 2.51x (#1) | 0.037 | 3.03x | 3.05x (#1) |
| 83 | `negative_limit_by_in_order` | 9 | 1.78 | 2.28 | 2.96x | 6.84x (#6) | 2.30 | 2.79x | 5.23x (#6) |
| 84 | `decimal_casts` (`Memory`) | 8 | 0.805 | 2.07 | 2.87x | 4.04x (#3) | 2.78 | 2.94x | 5.21x (#3) |
| 85 | `lz4` | 3 | 0.061 | 0.157 | 2.34x | 3.94x (#2) | 0.188 | 2.82x | 4.26x (#2) |
| 86 | `string_subcolumn_in_tuple` (`Memory`) | 2 | 0.010 | 0.020 | 1.97x | 1.98x (#0) | 0.028 | 2.75x | 2.76x (#0) |
| 87 | `top_k_through_array_join` | 10 | 0.308 | 0.612 | 2.39x | 3.46x (#1) | 0.647 | 2.73x | 4.17x (#1) |
| 88 | `date_time_64` | 6 | 0.738 | 1.07 | 2.10x | 5.41x (#0) | 1.33 | 2.71x | 6.13x (#0) |
| 89 | `insert_statistics` | 7 | 1.16 | 1.21 | 1.47x | 5.26x (#6) | 2.17 | 2.67x | 10x (#6) |
| 90 | `parse_illegal_datetime` (`Memory`) | 4 | 0.009 | 0.018 | 1.87x | 2.05x (#1) | 0.025 | 2.60x | 2.81x (#1) |
| 91 | `codecs_float_select` | 7 | 0.929 | 1.65 | 1.96x | 6.10x (#1) | 1.77 | 2.58x | 7.68x (#1) |
| 92 | `casts` (`Memory`) | 3 | 0.441 | 0.951 | 2.16x | 2.19x (#0) | 1.13 | 2.57x | 2.60x (#0) |
| 93 | `add_distinct_to_in_clause` | 2 | 3.14 | 4.12 | 2.14x | 4.07x (#1) | 4.59 | 2.55x | 5.47x (#1) |
| 94 | `top_k_through_join` | 3 | 0.154 | 0.216 | 1.77x | 3.88x (#0) | 0.294 | 2.53x | 7.06x (#0) |
| 95 | `enum_string_operation` | 5 | 1.68 | 3.96 | 2.35x | 2.49x (#4) | 4.37 | 2.50x | 2.96x (#4) |
| 96 | `optimize_time_filter_with_preimage` | 4 | 0.447 | 1.15 | 2.16x | 4.71x (#0) | 1.28 | 2.49x | 5.12x (#0) |
| 97 | `join_convert_full_to_left_or_right` | 1 | 0.008 | 0.015 | 1.84x | 1.84x (#0) | 0.020 | 2.48x | 2.48x (#0) |
| 98 | `key_condition_multi_atom_wide_or` | 1 | 0.021 | 0.043 | 2.01x | 2.01x (#0) | 0.052 | 2.44x | 2.44x (#0) |
| 99 | `hashed_dictionary_sharded` (`Memory`) | 3 | 0.038 | 0.092 | 2.42x | 2.42x (#4) | 2.67 | 1.29x | 1.35x (#0) |
| 100 | `insert_sparse_column` | 1 | 0.133 | 0.283 | 2.12x | 2.12x (#0) | 0.321 | 2.41x | 2.41x (#0) |
| 101 | `sparse_column` | 2 | 0.138 |  |  |  | 0.357 | 2.41x | 2.89x (#44) |
| 102 | `codecs_int_select` | 6 | 0.729 | 1.18 | 1.94x | 5.26x (#1) | 0.975 | 2.37x | 4.56x (#9) |
| 103 | `memory_cache_friendliness` | 1 | 0.119 | 0.209 | 1.76x | 1.76x (#0) | 0.280 | 2.36x | 2.36x (#0) |
| 104 | `sort` (`Memory`) | 10 | 1.55 | 2.61 | 2.17x | 49x (#0) | 2.66 | 2.29x | 50x (#0) |
| 105 | `sparse_column_index_sorted` | 2 | 0.902 | 2.19 | 2.27x | 2.61x (#1) | 2.20 | 2.28x | 2.61x (#1) |
| 106 | `array_enumerate_fixed_string` (`Memory`) | 2 | 0.034 | 0.077 | 2.23x | 2.29x (#0) | 0.078 | 2.27x | 2.32x (#0) |
| 107 | `join_filter_pushdown` | 1 | 0.008 | 0.014 | 1.75x | 1.75x (#0) | 0.018 | 2.26x | 2.26x (#0) |
| 108 | `like_perfect_affix_rewrite` | 2 | 0.190 | 0.408 | 2.15x | 2.16x (#1) | 0.429 | 2.26x | 2.26x (#0) |
| 109 | `low_cardinality_argument` | 4 | 0.029 | 0.049 | 1.66x | 1.85x (#1) | 0.065 | 2.25x | 2.53x (#1) |
| 110 | `tokenizers` (`Memory`) | 8 | 0.635 | 1.13 | 2.02x | 4.37x (#2) | 1.25 | 2.24x | 5.27x (#7) |
| 111 | `group_bitwise_if` (`Memory`) | 7 | 0.607 | 0.892 | 1.92x | 15x (#11) | 1.18 | 2.22x | 24x (#11) |
| 112 | `distinct_partitions` | 6 | 0.419 | 1.21 | 1.87x | 5.03x (#12) | 2.40 | 2.22x | 11x (#17) |
| 113 | `merge_tree_insert` | 3 | 0.895 | 0.626 | 1.12x | 3.84x (#18) | 0.571 | 2.21x | 2.87x (#7) |
| 114 | `creating_set_partitions` | 3 | 0.888 | 0.998 | 2.20x | 7.02x (#10) | 1.10 | 1.56x | 4.42x (#8) |
| 115 | `has_any_constant_string_needles` | 8 | 2.41 | 3.24 | 2.11x | 14x (#2) | 3.29 | 2.19x | 14x (#2) |
| 116 | `array_compact` (`Memory`) | 5 | 0.290 | 0.441 | 2.09x | 3.64x (#2) | 0.452 | 2.15x | 3.70x (#2) |
| 117 | `low_cardinality_query` | 2 | 0.018 | 0.032 | 1.83x | 1.84x (#0) | 0.037 | 2.11x | 2.15x (#0) |
| 118 | `map_subcolumns_prewhere` | 9 | 2.04 | 4.54 | 2.11x | 3.29x (#8) | 4.52 | 2.10x | 3.29x (#4) |
| 119 | `get_map_value` (`Memory`) | 3 | 0.331 | 0.662 | 2.05x | 2.72x (#0) | 0.665 | 2.06x | 2.72x (#0) |
| 120 | `text_index_low_cardinality_build` | 6 | 1.19 | 0.804 | 0.96x | 2.66x (#0) | 1.69 | 2.03x | 5.97x (#0) |
| 121 | `sorted_distinct` | 8 | 0.270 | 0.291 | 1.91x | 3.88x (#10) | 0.377 | 2.01x | 3.68x (#11) |
| 122 | `inverse_dictionary_lookup` | 6 | 0.028 | 0.048 | 1.70x | 1.74x (#17) | 0.037 | 2.01x | 2.04x (#17) |
| 123 | `join_merge_filter_into_join` | 2 | 0.013 | 0.020 | 1.57x | 1.59x (#0) | 0.026 | 2.01x | 2.05x (#0) |
| 124 | `string_to_int` | 1 | 0.002 | 0.002 | 1.31x | 1.31x (#0) | 0.003 | 2.00x | 2.00x (#0) |
| 125 | `prewhere_with_row_level_filter` | 5 | 0.564 | 1.00 | 1.76x | 1.99x (#2) | 1.11 | 1.95x | 2.12x (#2) |
| 126 | `aggregation_in_order_2` | 9 | 2.22 | 2.71 | 1.69x | 5.88x (#0) | 3.05 | 1.95x | 7.46x (#0) |
| 127 | `set_index_insert_fixed_keys` | 2 | 0.475 | 0.586 | 1.24x | 1.32x (#1) | 0.909 | 1.92x | 2.10x (#1) |
| 128 | `group_by_sundy_li` | 3 | 0.092 | 0.136 | 1.58x | 1.79x (#0) | 0.160 | 1.91x | 2.35x (#0) |
| 129 | `skip_index_order_after_partition_pruning` | 2 | 0.009 | 0.011 | 1.25x | 1.32x (#0) | 0.016 | 1.83x | 1.90x (#0) |
| 130 | `parallel_full_sorting_merge_join` | 8 | 1.29 | 2.04 | 1.73x | 6.26x (#4) | 2.10 | 1.80x | 6.35x (#4) |
| 131 | `has_any_has_all_nullable` | 6 | 1.57 | 0.685 | 1.29x | 19x (#24) | 0.646 | 1.79x | 20x (#24) |
| 132 | `distinct_low_cardinality` | 2 | 0.032 | 0.039 | 1.53x | 2.86x (#1) | 0.045 | 1.78x | 3.21x (#1) |
| 133 | `group_by_multi_low_cardinality` | 4 | 0.429 | 0.477 | 1.49x | 4.32x (#2) | 0.546 | 1.77x | 5.50x (#2) |
| 134 | `string_bytes_functions` | 2 | 2.53 | 2.72 | 1.69x | 2.75x (#0) | 2.74 | 1.76x | 2.98x (#0) |
| 135 | `text_index_analysis` | 8 | 0.120 | 0.098 | 1.47x | 2.23x (#4) | 0.117 | 1.76x | 2.63x (#4) |
| 136 | `lot_of_subcolumns` | 1 | 0.038 | 0.066 | 1.73x | 1.73x (#0) | 0.067 | 1.75x | 1.75x (#0) |
| 137 | `set_index_check_in_range` | 1 | 0.229 | 0.390 | 1.71x | 1.71x (#0) | 0.400 | 1.75x | 1.75x (#0) |
| 138 | `sum` (`Memory`) | 5 | 0.266 | 0.267 | 1.00x | 1.01x (#15) | 0.538 | 1.74x | 16x (#22) |
| 139 | `join_used_flags` (`Memory`) | 2 | 0.207 | 0.292 | 1.46x | 1.53x (#0) | 0.329 | 1.72x | 1.91x (#0) |
| 140 | `narrow_column_read_stream_limit` | 2 | 0.027 | 0.041 | 1.50x | 1.57x (#0) | 0.047 | 1.72x | 1.82x (#0) |
| 141 | `query_interpretation_wide_join` | 7 | 1.76 | 2.43 | 1.43x | 1.72x (#4) | 2.91 | 1.72x | 2.09x (#4) |
| 142 | `map_lowcardinality_key_lookup` | 5 | 0.764 | 1.21 | 1.65x | 2.25x (#4) | 1.25 | 1.71x | 2.39x (#4) |
| 143 | `join_convert_outer_to_inner_transitively` | 2 | 0.340 | 0.491 | 1.44x | 1.46x (#1) | 0.580 | 1.71x | 1.71x (#0) |
| 144 | `text_index_count_stream_resolution` | 2 | 0.045 | 0.063 | 1.43x | 1.56x (#0) | 0.075 | 1.69x | 1.85x (#0) |
| 145 | `count` | 3 | 0.080 | 0.093 | 1.39x | 2.00x (#0) | 0.101 | 1.68x | 3.09x (#0) |
| 146 | `aggregation_single_level_result_fanout` | 3 | 0.521 | 0.878 | 1.68x | 1.74x (#0) | 0.859 | 1.65x | 1.70x (#0) |
| 147 | `window_partitions` | 8 | 1.13 | 1.48 | 1.25x | 2.04x (#13) | 2.06 | 1.66x | 2.51x (#13) |
| 148 | `trim_whitespace` | 6 | 0.760 | 1.07 | 1.46x | 1.83x (#3) | 1.21 | 1.63x | 1.97x (#3) |
| 149 | `bloom_filter_insert_low_cardinality` | 2 | 1.03 | 1.06 | 1.36x | 2.53x (#0) | 1.26 | 1.62x | 3.11x (#0) |
| 150 | `low_cardinality_insert` | 1 | 0.161 | 0.150 | 0.93x | 0.93x (#0) | 0.259 | 1.61x | 1.61x (#0) |
| 151 | `distinct_low_cardinality_prelimit_distinct_in_order` | 6 | 0.303 | 0.228 | 1.60x | 2.69x (#4) | 0.222 | 1.59x | 2.78x (#4) |
| 152 | `sparse_column_filter` | 9 | 2.60 | 2.31 | 1.28x | 4.61x (#9) | 3.41 | 1.59x | 6.34x (#9) |
| 153 | `array_distinct_decimal` (`Memory`) | 8 | 0.232 | 0.297 | 1.48x | 2.47x (#5) | 0.318 | 1.59x | 2.78x (#5) |
| 154 | `array_distinct` (`Memory`) | 3 | 0.250 | 0.370 | 1.57x | 2.42x (#1) | 0.374 | 1.58x | 2.42x (#1) |
| 155 | `group_by_no_aggregates_prefetch` (`Memory`) | 3 | 2.87 | 3.69 | 1.39x | 1.66x (#1) | 4.17 | 1.58x | 1.88x (#1) |
| 156 | `parameterized_view_analysis` | 2 | 0.368 | 0.430 | 1.22x | 1.34x (#1) | 0.528 | 1.56x | 1.85x (#1) |
| 157 | `point_in_polygon_index` | 1 | 0.005 | 0.007 | 1.27x | 1.27x (#0) | 0.008 | 1.56x | 1.56x (#0) |
| 158 | `set_index_analysis` | 1 | 0.007 | 0.009 | 1.17x | 1.17x (#0) | 0.012 | 1.55x | 1.55x (#0) |
| 159 | `memory_prewhere` (`Memory`) | 3 | 0.001 | 0.002 | 1.54x | 1.54x (#4) | 0.032 | 1.18x | 2.62x (#4) |
| 160 | `distinct_in_order_final_prefix` | 1 | 0.217 | 0.330 | 1.52x | 1.52x (#0) | 0.329 | 1.51x | 1.51x (#0) |
| 161 | `multiif` | 4 | 1.47 | 3.88 | 1.33x | 3.01x (#0) | 4.68 | 1.47x | 3.66x (#0) |
| 162 | `minmax_index_insert` | 1 | 0.356 | 0.339 | 0.95x | 0.95x (#0) | 0.521 | 1.46x | 1.46x (#0) |
| 163 | `logical_functions_small` (`Memory`) | 8 | 1.06 | 2.38 | 1.46x | 2.79x (#4) | 0.961 | 1.24x | 2.92x (#2) |
| 164 | `partial_merge_join` | 4 | 0.056 | 0.073 | 1.25x | 1.57x (#0) | 0.154 | 1.45x | 1.95x (#0) |
| 165 | `logical_functions_medium` (`Memory`) | 5 | 0.313 | 0.788 | 1.44x | 3.83x (#16) | 0.674 | 1.42x | 3.76x (#16) |
| 166 | `group_by_string_key_count` (`Memory`) | 1 | 0.362 | 0.461 | 1.27x | 1.27x (#0) | 0.520 | 1.44x | 1.44x (#0) |
| 167 | `array_join_filter_fusion` | 7 | 0.720 | 0.965 | 1.39x | 1.97x (#6) | 0.997 | 1.43x | 2.01x (#6) |
| 168 | `join_lazy_replication_sort` | 3 | 0.764 | 1.09 | 1.42x | 1.79x (#2) | 1.08 | 1.42x | 1.85x (#2) |
| 169 | `join_key_only_hash_tables` (`Memory`) | 10 | 1.74 | 2.19 | 1.34x | 1.70x (#1) | 2.24 | 1.39x | 1.94x (#6) |
| 170 | `extract_key_value_pairs` (`Memory`) | 10 | 1.55 | 1.97 | 1.36x | 1.67x (#2) | 2.01 | 1.39x | 1.72x (#2) |
| 171 | `reverse_string` (`Memory`) | 1 | 0.029 | 0.040 | 1.38x | 1.38x (#0) | 0.039 | 1.37x | 1.37x (#0) |
| 172 | `all_join_opt` | 7 | 0.263 | 0.327 | 1.24x | 1.48x (#2) | 0.365 | 1.38x | 1.69x (#2) |
| 173 | `simple_join_query` | 4 | 0.166 | 0.193 | 1.17x | 1.18x (#2) | 0.229 | 1.38x | 1.53x (#0) |
| 174 | `hashjoin_packed_keys` (`Memory`) | 4 | 1.42 | 1.88 | 1.32x | 1.83x (#1) | 1.94 | 1.37x | 1.90x (#1) |
| 175 | `IPv4` | 4 | 0.118 | 0.152 | 1.30x | 1.34x (#0) | 0.160 | 1.37x | 1.42x (#0) |
| 176 | `inserts_arrays_lowcardinality` | 2 | 0.636 | 0.341 | 0.66x | 1.28x (#0) | 0.708 | 1.36x | 2.63x (#0) |
| 177 | `group_by_fixed_keys` | 7 | 0.797 | 0.730 | 1.01x | 2.00x (#5) | 0.701 | 1.36x | 2.26x (#7) |
| 178 | `aggregation_by_partitions` | 7 | 0.085 | 0.090 | 1.24x | 2.00x (#15) | 0.175 | 1.34x | 2.94x (#12) |
| 179 | `parquet_write_bloom_filter` (`Memory`) | 2 | 0.104 | 0.130 | 1.27x | 1.32x (#0) | 0.138 | 1.34x | 1.44x (#0) |
| 180 | `array_join_function_lowering` | 3 | 0.278 | 0.286 | 1.27x | 2.11x (#2) | 0.282 | 1.34x | 2.56x (#2) |
| 181 | `aggregation_low_cardinality` | 2 | 0.068 | 0.084 | 1.23x | 1.24x (#0) | 0.091 | 1.33x | 1.37x (#0) |
| 182 | `timeseries_to_grid_bucketing` | 7 | 1.51 | 1.76 | 1.23x | 1.71x (#0) | 1.82 | 1.32x | 1.71x (#0) |
| 183 | `array_bulk_insert` (`Memory`) | 1 | 0.081 | 0.105 | 1.31x | 1.31x (#0) | 0.101 | 1.26x | 1.26x (#0) |
| 184 | `statistics_simple_if` (`Memory`) | 8 | 1.10 | 1.33 | 1.30x | 6.19x (#12) | 1.58 | 1.29x | 7.59x (#12) |
| 185 | `string_join` | 4 | 1.33 | 1.59 | 1.19x | 1.23x (#3) | 1.74 | 1.29x | 1.35x (#1) |
| 186 | `group_by_low_cardinality_short_strings` | 4 | 0.171 | 0.207 | 1.22x | 1.31x (#1) | 0.218 | 1.28x | 1.35x (#1) |
| 187 | `group_by_no_aggregates_mapped_keys` (`Memory`) | 3 | 0.683 | 0.763 | 1.10x | 1.32x (#2) | 0.880 | 1.28x | 1.45x (#2) |
| 188 | `join_convert_to_fixed_hash_table` | 2 | 0.106 | 0.116 | 1.10x | 1.11x (#26) | 0.109 | 1.28x | 1.28x (#30) |
| 189 | `right_full_hash_join` | 8 | 1.39 | 1.55 | 1.12x | 1.18x (#1) | 1.73 | 1.26x | 1.38x (#1) |
| 190 | `dict_join` | 2 | 0.073 | 0.087 | 1.26x | 1.38x (#0) | 0.082 | 1.23x | 1.46x (#0) |
| 191 | `countIf` | 4 | 0.150 | 0.162 | 1.19x | 1.98x (#3) | 0.167 | 1.26x | 2.41x (#3) |
| 192 | `preliminary_distinct_abandoning` | 2 | 0.369 | 0.406 | 1.23x | 1.68x (#0) | 0.416 | 1.25x | 1.69x (#0) |
| 193 | `nlp` | 3 | 1.09 | 1.70 | 1.24x | 1.92x (#2) | 1.71 | 1.24x | 1.94x (#2) |
| 194 | `bloom_filter_insert` | 1 | 0.198 | 0.158 | 0.80x | 0.80x (#0) | 0.244 | 1.23x | 1.23x (#0) |
| 195 | `set_in_clustered_keys` | 4 | 1.91 | 1.97 | 1.12x | 1.38x (#2) | 2.02 | 1.21x | 1.64x (#2) |
| 196 | `join_lazy_indexing` | 2 | 0.651 | 0.790 | 1.21x | 1.25x (#37) |  |  |  |
| 197 | `hashed_array_dictionary` (`Memory`) | 5 | 0.622 | 0.717 | 1.17x | 1.25x (#6) | 0.826 | 1.20x | 1.23x (#6) |
| 198 | `order_by_single_nullable` (`Memory`) | 2 | 2.56 | 2.81 | 1.12x | 1.22x (#0) | 3.00 | 1.19x | 1.30x (#0) |
| 199 | `json_string_value_formatting` (`Memory`) | 6 | 2.14 | 2.40 | 1.17x | 1.28x (#0) | 2.43 | 1.19x | 1.30x (#2) |
| 200 | `join_squashing_blocks` | 3 | 0.280 | 0.320 | 1.14x | 1.28x (#2) | 0.332 | 1.18x | 1.30x (#2) |
| 201 | `hierarchical_dictionaries` | 10 | 3.19 | 3.47 | 1.13x | 1.33x (#2) | 3.59 | 1.17x | 1.41x (#2) |
| 202 | `group_by_multiple_strings` | 4 | 0.155 | 0.162 | 1.02x | 1.06x (#29) | 0.375 | 1.17x | 1.52x (#3) |
| 203 | `limit_by_with_order` | 5 | 1.24 | 1.05 | 1.15x | 6.93x (#7) | 0.339 | 1.17x | 7.09x (#7) |
| 204 | `string_order_by` (`Memory`) | 6 | 4.38 | 4.98 | 1.13x | 1.19x (#2) | 5.11 | 1.17x | 1.30x (#5) |
| 205 | `dict_get_keys_const_path` | 4 | 0.537 | 0.560 | 1.04x | 1.07x (#1) | 0.625 | 1.15x | 1.23x (#3) |
| 206 | `parallel_hash_join_various` (`Memory`) | 6 | 2.39 | 2.36 | 1.12x | 1.74x (#5) | 2.37 | 1.15x | 1.97x (#5) |
| 207 | `window_functions_sliding_frames` | 8 | 7.29 | 7.61 | 1.05x | 1.14x (#0) | 9.45 | 1.15x | 1.87x (#7) |
| 208 | `sparse_column_index_runs` | 6 | 0.536 | 0.563 | 1.13x | 1.58x (#5) | 0.570 | 1.15x | 1.57x (#5) |
| 209 | `json_key_formatting` (`Memory`) | 4 | 2.40 | 2.52 | 1.06x | 1.09x (#3) | 2.71 | 1.14x | 1.17x (#3) |
| 210 | `uniq_with_key` | 8 | 2.72 | 3.03 | 1.13x | 1.97x (#6) | 3.07 | 1.13x | 1.57x (#4) |
| 211 | `join_any_build_duplicate_keys` (`Memory`) | 3 | 0.562 | 0.611 | 1.09x | 1.10x (#1) | 0.625 | 1.11x | 1.13x (#2) |
| 212 | `hash_table_iteration_prefetch` (`Memory`) | 1 | 0.646 | 0.717 | 1.11x | 1.11x (#0) | 0.712 | 1.10x | 1.10x (#0) |
| 213 | `sort_wide_table` | 4 | 1.74 | 1.58 | 0.96x | 1.10x (#1) | 1.80 | 1.11x | 1.25x (#3) |
| 214 | `hash_join_with_row_store` | 6 | 8.92 | 9.18 | 0.96x | 1.13x (#17) | 8.11 | 1.10x | 1.26x (#16) |
| 215 | `sort_multi_column_mixed` | 6 | 1.83 | 1.86 | 1.05x | 1.18x (#3) | 1.95 | 1.10x | 1.22x (#1) |
| 216 | `text_index_pattern_bypass_cache` | 2 | 0.022 | 0.021 | 0.98x | 1.04x (#0) | 0.024 | 1.09x | 1.15x (#0) |
| 217 | `sort_low_cardinality` | 8 | 1.57 | 1.46 | 1.06x | 2.19x (#0) | 1.47 | 1.09x | 2.20x (#0) |
| 218 | `window_functions_qualify` | 5 | 3.41 | 3.62 | 1.06x | 1.10x (#4) | 3.72 | 1.09x | 1.14x (#4) |
| 219 | `joined_block_split_single_row` | 3 | 0.138 | 0.145 | 1.05x | 1.05x (#1) | 0.150 | 1.09x | 1.10x (#2) |
| 220 | `window_functions_ranking` | 9 | 7.57 | 8.21 | 1.08x | 1.10x (#0) | 8.34 | 1.08x | 1.10x (#0) |
| 221 | `array_join` | 9 | 1.31 | 1.36 | 1.07x | 1.33x (#9) | 1.53 | 0.96x | 1.47x (#9) |
| 222 | `aggregate_with_serialized_method` (`Memory`) | 5 | 2.17 | 2.33 | 1.07x | 1.08x (#4) | 2.30 | 1.06x | 1.07x (#1) |
| 223 | `hashed_dictionary_load_factor` (`Memory`) | 5 | 2.59 | 2.76 | 1.07x | 1.15x (#7) | 2.11 | 0.95x | 1.12x (#0) |
| 224 | `wide_table_nested_select` | 3 | 0.517 | 0.544 | 1.07x | 1.11x (#0) | 0.542 | 1.07x | 1.12x (#0) |
| 225 | `window_functions_many_small_partitions` | 7 | 5.89 | 6.21 | 1.06x | 1.09x (#3) | 6.26 | 1.07x | 1.10x (#3) |
| 226 | `sort_nearly_sorted` | 2 | 0.518 | 0.474 | 0.99x | 1.12x (#1) | 0.514 | 1.06x | 1.19x (#1) |
| 227 | `hashed_dictionary` (`Memory`) | 9 | 0.859 | 0.849 | 0.99x | 1.00x (#0) | 0.963 | 1.06x | 1.12x (#1) |
| 228 | `uniq_with_key_dist` | 8 | 1.11 | 1.17 | 1.05x | 1.13x (#2) | 1.22 | 1.01x | 1.11x (#2) |
| 229 | `jit_sort` | 8 | 8.43 | 8.66 | 1.03x | 1.08x (#4) | 7.15 | 1.05x | 1.11x (#4) |
| 230 | `direct_dictionary` (`Memory`) | 3 | 2.34 | 2.41 | 1.03x | 1.04x (#17) | 2.24 | 1.03x | 1.06x (#9) |
| 231 | `sort_string_full` | 4 | 2.58 | 2.48 | 1.00x | 1.10x (#3) | 2.58 | 1.03x | 1.12x (#1) |
| 232 | `hashjoin_with_large_output` (`Memory`) | 3 | 0.642 | 0.650 | 1.01x | 1.02x (#2) | 0.658 | 1.03x | 1.03x (#2) |
| 233 | `dict_get_keys_vector_path` | 2 | 1.96 | 2.00 | 1.02x | 1.03x (#1) | 1.90 | 0.97x | 0.99x (#0) |
| 234 | `right_anti_join_with_residual_filter` | 4 | 0.888 | 0.873 | 0.99x | 1.04x (#3) | 0.904 | 1.02x | 1.07x (#3) |
| 235 | `subqueries` | 2 | 0.408 | 0.411 | 1.01x | 1.01x (#1) | 0.417 | 1.02x | 1.02x (#0) |
| 236 | `uniq_without_key` | 6 | 0.298 | 0.297 | 1.01x | 1.08x (#21) | 0.607 | 1.00x | 1.17x (#22) |
| 237 | `storage_join_direct_join` | 1 | 0.242 | 0.206 | 0.85x | 0.85x (#0) | 0.242 | 1.00x | 1.00x (#0) |
| 238 | `range_hashed_dictionary` (`Memory`) | 10 | 1.59 | 1.59 | 1.00x | 1.06x (#8) | 1.41 | 0.88x | 1.03x (#9) |
| 239 | `merge_tree_many_partitions` | 1 | 0.003 | 0.002 | 0.55x | 0.55x (#0) | 0.003 | 1.00x | 1.00x (#0) |
| 240 | `uniq_without_key_dist` | 5 | 1.20 | 1.12 | 0.90x | 1.00x (#2) | 1.23 | 0.99x | 1.07x (#4) |
| 241 | `point_in_polygon_3d_huge_multipolygon` | 1 | 0.034 | 0.030 | 0.90x | 0.90x (#0) | 0.033 | 0.99x | 0.99x (#0) |
| 242 | `flat_dictionary` (`Memory`) | 9 | 1.70 | 1.68 | 0.99x | 1.02x (#8) | 1.59 | 0.93x | 0.99x (#8) |
| 243 | `IPv6` | 2 | 0.303 | 0.292 | 0.96x | 0.97x (#1) | 0.298 | 0.98x | 0.99x (#0) |
| 244 | `aggregate_partitions_independently_default` | 4 | 0.420 | 0.366 | 0.90x | 1.00x (#2) | 0.394 | 0.98x | 1.09x (#2) |
| 245 | `aggregation_two_level_bucket_merge` | 4 | 3.45 | 3.25 | 0.97x | 1.18x (#3) | 3.24 | 0.98x | 1.21x (#3) |
| 246 | `native_arrow_ipc` (`Memory`) | 2 | 1.61 | 1.56 | 0.96x | 1.23x (#0) | 1.60 | 0.97x | 1.31x (#0) |
| 247 | `window_functions_downstream_multithreading` | 6 | 3.62 | 3.50 | 0.96x | 1.17x (#5) | 3.39 | 0.94x | 1.24x (#5) |
| 248 | `apply_patch_parts_steps` | 4 | 0.620 | 1.54 | 0.79x | 5.69x (#1) | 1.59 | 0.96x | 5.94x (#1) |
| 249 | `correlated_exists_many_keys` | 1 | 0.509 | 0.482 | 0.95x | 0.95x (#0) | 0.480 | 0.94x | 0.94x (#0) |
| 250 | `formats_columns_nullable` | 1 | 0.291 | 0.271 | 0.93x | 0.93x (#24) |  |  |  |
| 251 | `left_array_join_long_arrays` | 6 | 1.91 | 1.70 | 0.92x | 1.12x (#5) | 1.70 | 0.92x | 1.05x (#5) |
| 252 | `uniqExact_with_key_parallel_merging` | 4 | 2.24 | 1.80 | 0.83x | 0.89x (#1) | 1.88 | 0.87x | 0.98x (#3) |
| 253 | `groupby_onekey_nullable` | 9 | 1.78 | 1.29 | 0.71x | 0.85x (#6) | 1.54 | 0.86x | 0.96x (#7) |
| 254 | `query_condition_cache_topk` | 2 | 0.090 | 0.012 | 0.50x | 2.93x (#0) | 0.019 | 0.78x | 4.13x (#0) |
| 255 | `bloom_filter_zstd` | 2 | 0.036 | 0.028 | 0.55x | 1.14x (#1) | 0.043 | 0.73x | 1.78x (#1) |
| 256 | `text_index_build` | 9 | 3.53 | 1.07 | 0.45x | 0.83x (#3) | 1.68 | 0.72x | 1.39x (#3) |
| 257 | `timeseries_to_grid_scalar_add` | 3 | 0.465 | 0.292 | 0.72x | 0.93x (#2) | 0.213 | 0.62x | 0.84x (#1) |
| 258 | `permuted_columns_cache` | 1 | 0.637 | 0.270 | 0.42x | 0.42x (#0) | 0.429 | 0.67x | 0.67x (#0) |
| 259 | `projection_index_in_read_pools` | 4 | 0.134 | 0.064 | 0.50x | 0.75x (#1) | 0.085 | 0.67x | 1.02x (#1) |
| 260 | `aggregation_in_order_partial_key` | 10 | 1.49 | 0.845 | 0.64x | 1.39x (#5) | 0.859 | 0.67x | 1.56x (#5) |
| 261 | `set_disable_skip_index` | 1 | 0.032 | 0.018 | 0.58x | 0.58x (#0) | 0.020 | 0.65x | 0.65x (#0) |
| 262 | `window_function_with_order` | 4 | 1.23 | 0.441 | 0.50x | 1.20x (#1) | 0.463 | 0.53x | 1.26x (#1) |
| 263 | `subcolumn_deserialization_tuple_map` | 10 | 4.62 | 2.06 | 0.47x | 0.75x (#6) | 2.33 | 0.52x | 0.86x (#6) |
| 264 | `text_index_postprocessor_filter` | 4 | 4.46 | 1.36 | 0.30x | 0.34x (#2) | 2.06 | 0.46x | 0.53x (#2) |
| 265 | `prefetch_in_aggregation` | 7 | 13.44 | 1.85 | 0.37x | 1.03x (#11) | 2.15 | 0.45x | 1.08x (#10) |
| 266 | `text_index_like_large_dictionary` | 7 | 3.40 | 1.80 | 0.37x | 1.07x (#5) | 1.92 | 0.41x | 1.12x (#5) |
| 267 | `memory_bound_merging` | 1 | 0.385 |  |  |  | 0.153 | 0.40x | 0.40x (#0) |
| 268 | `insert_statistics_countmin` | 4 | 2.14 | 0.227 | 0.11x | 0.35x (#3) | 0.812 | 0.40x | 1.23x (#3) |
| 269 | `statistics_plan_time_uniq` | 2 | 0.430 | 0.165 | 0.37x | 0.39x (#1) | 0.164 | 0.37x | 0.39x (#1) |
| 270 | `constant_column_after_filter` | 2 | 0.730 | 0.248 | 0.34x | 0.36x (#0) | 0.273 | 0.37x | 0.40x (#0) |
| 271 | `skip_indexes_in_read_pools` | 4 | 0.204 | 0.059 | 0.29x | 0.36x (#1) | 0.074 | 0.37x | 0.45x (#1) |
| 272 | `sparse_grams_index_build` | 5 | 9.33 | 2.71 | 0.25x | 1.12x (#3) | 3.11 | 0.35x | 1.10x (#3) |
| 273 | `top_k_threshold_tracker` | 2 | 0.134 | 0.026 | 0.20x | 0.20x (#0) | 0.072 | 0.27x | 0.27x (#0) |
| 274 | `aggregating_merge_tree_simple_aggregate_function_string` | 1 | 0.224 | 0.036 | 0.16x | 0.16x (#0) | 0.041 | 0.18x | 0.18x (#0) |
| 275 | `parallel_index` | 3 | 0.660 | 0.039 | 0.17x | 1.81x (#2) | 0.043 | 0.072x | 0.12x (#0) |
| 276 | `text_index_build_tokenizers` | 3 | 3.41 | 0.305 | 0.090x | 0.094x (#1) | 0.565 | 0.17x | 0.18x (#1) |
| 277 | `array_index_low_cardinality_strings` | 10 | 6.01 | 0.760 | 0.12x | 0.22x (#0) | 0.846 | 0.14x | 0.24x (#0) |
| 278 | `long_primary_key` | 3 | 0.430 | 0.040 | 0.092x | 0.094x (#0) | 0.047 | 0.11x | 0.11x (#2) |
| 279 | `alter_select` | 4 | 1.73 | 0.015 | 0.045x | 0.38x (#0) | 0.019 | 0.060x | 0.54x (#0) |
| 280 | `partition_pruning_trace_formatting` | 1 | 0.102 | 0.004 | 0.035x | 0.035x (#0) | 0.005 | 0.049x | 0.049x (#0) |
| 281 | `merge_tree_many_partitions_2` | 1 | 0.188 | 0.005 | 0.027x | 0.027x (#0) | 0.007 | 0.037x | 0.037x (#0) |
| 282 | `insert_sequential_and_background_merges` | 1 | 2.27 | 0.068 | 0.030x | 0.030x (#0) | 0.078 | 0.034x | 0.034x (#0) |
| 283 | `merge_tree_small_blocks` | 5 | 3.39 | 0.068 | 0.019x | 0.027x (#0) | 0.080 | 0.023x | 0.029x (#0) |
| 284 | `adaptive_codec_merge` | 3 | 2.70 | 0.008 | 0.003x | 0.003x (#0) | 0.014 | 0.005x | 0.006x (#0) |
