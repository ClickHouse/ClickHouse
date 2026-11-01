# Iceberg vs MergeTree in the performance tests {#iceberg-vs-mergetree}

The run time of the same perf test queries with the MergeTree tables of the test (the usual configuration) and with them created as Iceberg tables (`perf.py --table-engine`, see the "Iceberg configuration" section of `README.md`). A snapshot from the CI of https://github.com/ClickHouse/ClickHouse/pull/128261, commit aa299f4c73ba740aabd68bde4871bd377481302e, run on 2026-11-01.

Source: `all-query-metrics.tsv` of the twelve ARM shards of that run: `Performance Comparison (arm_release, master_head, N/6)` for MergeTree, `(arm_release, master_head, iceberg_local, N/3)` and `(arm_release, master_head, iceberg_s3, N/3)` for Iceberg. Dashboard: https://performance.ci.clickhouse.com/runs/pr-128261-aa299f4c73ba740aabd68bde4871bd377481302e

How the numbers are computed:

* A test marked (`Memory`) has `Memory` tables in the usual configuration: its MergeTree column reads RAM, not storage, so its ratio is mostly the cost of reading and decoding Parquet.
* The time of a query is the median `client_time` of the tested server in its shard. All three configurations ran the same binary on the same runner type (`m8g.4xlarge`), but on different hosts.
* A pull request run measures up to 10 queries per test, so only the queries measured in both configurations are compared: 1157 of the `IcebergLocal` queries and 1153 of the `IcebergS3` ones.
* The slowdown of a test is the geometric mean over its queries of Iceberg time / MergeTree time; above 1x the Iceberg tables are slower. The time columns are the sums of the query medians, in seconds. "Worst query" is the largest per-query ratio, with its `query_index` (`perf.py --print-queries tests/performance/<test>.xml --queries-to-run <query_index>` prints it).
* `IcebergS3` writes to and reads from the job-local SeaweedFS, not a remote object store.

The ratios are not regressions. Many of these tests measure a MergeTree feature that an Iceberg table does not have (the primary key, skip and text indexes, projections, read in order, lightweight deletes), so a large ratio often means that the feature is not there. The ratios far below 1x are measured `OPTIMIZE` and `INSERT` statements that do much less work on an Iceberg table (no merge of parts, no index to build). The first rows are the places where an Iceberg table loses most against the same data in MergeTree; the comparison between two commits of the Iceberg configuration itself is in its own CI jobs and on the dashboard under `arm_iceberg_local` and `arm_iceberg_s3`.

## Summary {#summary}

| Backend | Tests | Queries | Median per-test slowdown | Geometric mean | Tests > 2x | Tests > 10x | Tests faster on Iceberg |
|---|---|---|---|---|---|---|---|
| `IcebergLocal` | 272 | 1157 | 1.43x | 1.56x | 94 | 16 | 65 |
| `IcebergS3` | 270 | 1153 | 1.62x | 1.76x | 108 | 15 | 55 |

## Per test, sorted by slowdown {#per-test}

| # | Test | Queries | MergeTree, s | `IcebergLocal`, s | `IcebergLocal` slowdown | Worst query (local) | `IcebergS3`, s | `IcebergS3` slowdown | Worst query (S3) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `ie_join_selectivity_key_choice` | 1 | 0.013 | 2.45 | 184x | 184x (#0) | 2.40 | 181x | 181x (#0) |
| 2 | `lightweight_delete` | 1 | 0.006 | 0.477 | 75x | 75x (#0) | 0.465 | 73x | 73x (#0) |
| 3 | `propagate_predicate_across_equi_join` | 5 | 0.650 | 3.37 | 55x | 190x (#0) | 3.26 | 53x | 177x (#0) |
| 4 | `point_in_polygon` (`Memory`) | 1 | 0.090 | 3.06 | 34x | 34x (#0) | 3.37 | 38x | 38x (#0) |
| 5 | `sparse_column` | 1 | 0.002 | 0.051 | 30x | 30x (#79) |  |  |  |
| 6 | `text_index_multi_search_any` | 4 | 0.018 | 0.497 | 27x | 30x (#3) | 0.515 | 29x | 31x (#2) |
| 7 | `optimize_functions_to_subcolumns` | 6 | 0.044 | 1.48 | 27x | 63x (#2) | 1.49 | 28x | 61x (#2) |
| 8 | `join_seal_gated_reading` | 6 | 0.109 | 2.21 | 20x | 26x (#2) | 2.40 | 22x | 28x (#2) |
| 9 | `jit_large_requests` (`Memory`) | 2 | 0.211 | 3.71 | 18x | 22x (#1) | 3.94 | 19x | 23x (#1) |
| 10 | `apply_patch_parts_merge` | 4 | 0.613 | 9.94 | 17x | 22x (#0) | 10.19 | 17x | 21x (#0) |
| 11 | `cast_nullable_read_in_order` | 2 | 0.012 | 0.175 | 15x | 15x (#1) | 0.193 | 16x | 16x (#1) |
| 12 | `array_index_simd_numeric` (`Memory`) | 3 | 0.075 | 0.658 | 15x | 57x (#1) | 0.658 | 15x | 58x (#1) |
| 13 | `codec_none` | 1 | 0.005 | 0.066 | 13x | 13x (#0) | 0.078 | 15x | 15x (#0) |
| 14 | `opt_max_min` (`Memory`) | 9 | 0.090 | 0.916 | 10x | 16x (#5) | 1.15 | 14x | 21x (#5) |
| 15 | `text_index_posting_list_codec` | 6 | 0.023 | 0.186 | 8.05x | 10x (#14) | 0.162 | 14x | 15x (#5) |
| 16 | `array_first_last` (`Memory`) | 8 | 0.101 | 1.06 | 12x | 32x (#3) | 1.16 | 11x | 31x (#2) |
| 17 | `text_index_lazy_wide_granule` | 5 | 0.115 | 1.10 | 10x | 20x (#12) | 0.906 | 9.21x | 9.77x (#1) |
| 18 | `map_keys_values_filter_only` | 4 | 0.861 | 5.88 | 9.47x | 23x (#0) | 5.87 | 9.46x | 23x (#0) |
| 19 | `map_contains_value_subcolumn` | 3 | 1.79 | 7.05 | 9.07x | 23x (#0) | 7.32 | 9.42x | 24x (#0) |
| 20 | `map_contains_like_subcolumn` | 10 | 0.626 | 4.10 | 8.78x | 26x (#2) | 4.15 | 8.89x | 26x (#2) |
| 21 | `subsecond_extractors` (`Memory`) | 3 | 0.517 | 3.62 | 7.00x | 7.25x (#1) | 4.35 | 8.40x | 8.70x (#1) |
| 22 | `memory_compressed_subcolumns` (`Memory`) | 3 | 0.042 | 0.321 | 7.67x | 7.79x (#0) | 0.343 | 8.20x | 8.38x (#0) |
| 23 | `least_greatest_nullable_variadic` (`Memory`) | 7 | 0.043 | 0.287 | 6.53x | 7.72x (#0) | 0.357 | 8.16x | 9.58x (#1) |
| 24 | `limit_by_partitions` | 5 | 0.244 | 2.91 | 7.72x | 42x (#1) | 1.59 | 8.06x | 38x (#1) |
| 25 | `text_index_phrase_cursor` | 7 | 0.145 | 0.686 | 6.95x | 12x (#0) | 0.746 | 7.57x | 13x (#0) |
| 26 | `codecs_float_insert` | 4 | 1.75 | 3.94 | 2.27x | 3.13x (#7) | 9.05 | 7.52x | 14x (#3) |
| 27 | `array_join_nested_columns` (`Memory`) | 2 | 0.019 | 0.110 | 6.76x | 12x (#0) | 0.112 | 6.90x | 13x (#0) |
| 28 | `array_distance_simd_speedup` (`Memory`) | 8 | 0.369 | 2.29 | 6.82x | 11x (#2) | 2.18 | 6.63x | 11x (#4) |
| 29 | `read_in_order_virtual_row_filtered` | 2 | 0.006 | 0.039 | 6.12x | 6.12x (#0) | 0.043 | 6.78x | 6.78x (#0) |
| 30 | `materialized_view_deduplication` | 1 | 0.008 | 0.022 | 2.74x | 2.74x (#0) | 0.053 | 6.46x | 6.46x (#0) |
| 31 | `mmap_io` | 1 | 0.004 | 0.016 | 3.92x | 3.92x (#0) | 0.024 | 6.00x | 6.00x (#0) |
| 32 | `zstd_codec_context_reuse` | 3 | 1.45 | 1.62 | 0.22x | 2.61x (#0) | 4.19 | 5.96x | 7.42x (#1) |
| 33 | `codecs_int_insert` | 6 | 0.428 | 1.29 | 3.41x | 7.61x (#5) | 2.19 | 5.95x | 9.66x (#4) |
| 34 | `key_condition_multi_expression_key` | 2 | 0.005 | 0.024 | 4.88x | 5.48x (#0) | 0.029 | 5.86x | 6.81x (#0) |
| 35 | `low_cardinality_many_marks_point_lookups` | 1 | 0.285 | 1.42 | 5.00x | 5.00x (#0) | 1.65 | 5.80x | 5.80x (#0) |
| 36 | `enum_in_set` | 1 | 0.277 | 1.40 | 5.07x | 5.07x (#0) | 1.58 | 5.69x | 5.69x (#0) |
| 37 | `low_cardinality_nested_function_fast_paths` | 5 | 0.063 | 0.282 | 4.50x | 5.16x (#1) | 0.348 | 5.50x | 6.47x (#1) |
| 38 | `full_sorting_merge_join` | 6 | 0.176 | 1.20 | 5.20x | 12x (#13) | 1.23 | 5.43x | 12x (#13) |
| 39 | `constant_sort_key_insert` | 8 | 0.091 | 0.280 | 3.19x | 3.66x (#7) | 0.371 | 5.38x | 6.49x (#0) |
| 40 | `isValidASCII` (`Memory`) | 7 | 0.046 | 0.216 | 4.67x | 15x (#6) | 0.218 | 5.30x | 14x (#6) |
| 41 | `text_index_single_block_postings` | 6 | 0.039 | 0.146 | 3.95x | 5.35x (#0) | 0.199 | 5.25x | 6.17x (#0) |
| 42 | `text_index_prewhere_read_step_order` | 2 | 0.025 | 0.112 | 4.60x | 5.75x (#0) | 0.129 | 5.18x | 7.69x (#0) |
| 43 | `hashjoin_low_cardinality_key` (`Memory`) | 2 | 0.041 | 0.186 | 4.56x | 4.56x (#1) | 0.211 | 5.15x | 5.16x (#1) |
| 44 | `limit_by_in_pk_order` | 9 | 1.19 | 10.44 | 4.71x | 24x (#10) | 9.81 | 5.10x | 23x (#10) |
| 45 | `sort_patterns` (`Memory`) | 8 | 3.01 | 12.34 | 4.36x | 7.21x (#0) | 14.22 | 5.02x | 8.36x (#0) |
| 46 | `delta_decode` | 9 | 0.454 | 2.34 | 4.43x | 7.93x (#2) | 3.11 | 4.87x | 8.41x (#6) |
| 47 | `parallel_mv` | 1 | 0.268 | 0.899 | 3.36x | 3.36x (#0) | 1.28 | 4.79x | 4.79x (#0) |
| 48 | `join_filter_pushdown_equivalent_sets` | 5 | 0.025 | 0.095 | 3.83x | 4.18x (#3) | 0.116 | 4.69x | 5.11x (#3) |
| 49 | `string_filters_during_scan` | 5 | 0.237 | 1.25 | 3.60x | 6.79x (#1) | 1.60 | 4.64x | 8.64x (#3) |
| 50 | `map_has_subcolumn` | 4 | 0.490 | 1.84 | 4.26x | 7.32x (#2) | 1.93 | 4.45x | 7.64x (#2) |
| 51 | `key_condition_single_column_key_point_lookup` | 2 | 0.007 | 0.023 | 3.49x | 3.89x (#1) | 0.029 | 4.39x | 4.74x (#1) |
| 52 | `text_index_like_array` | 9 | 0.102 | 0.430 | 3.71x | 13x (#6) | 0.492 | 4.36x | 13x (#6) |
| 53 | `array_distance` (`Memory`) | 4 | 0.414 | 1.31 | 4.17x | 6.23x (#25) | 1.49 | 4.26x | 11x (#9) |
| 54 | `skip_index_planning_with_patch_parts` | 2 | 0.036 | 0.080 | 2.24x | 2.26x (#0) | 0.146 | 4.08x | 4.12x (#0) |
| 55 | `codecs_multiple_select` | 1 | 0.920 | 2.81 | 3.05x | 3.05x (#0) | 3.69 | 4.01x | 4.01x (#0) |
| 56 | `codecs_int_select` | 7 | 0.587 | 1.30 | 2.60x | 6.49x (#13) | 1.45 | 3.98x | 7.65x (#13) |
| 57 | `key_condition_multi_atom_large_in` | 1 | 0.011 | 0.035 | 3.06x | 3.06x (#0) | 0.045 | 3.95x | 3.95x (#0) |
| 58 | `lazy_materialization_join` | 7 | 0.623 | 1.81 | 2.77x | 11x (#2) | 2.64 | 3.94x | 16x (#2) |
| 59 | `estimate_compression_ratio_t64` | 2 | 0.012 | 0.037 | 3.23x | 3.29x (#0) | 0.046 | 3.93x | 3.96x (#0) |
| 60 | `clamp_numeric` (`Memory`) | 3 | 0.020 | 0.065 | 3.25x | 3.95x (#0) | 0.076 | 3.83x | 4.42x (#0) |
| 61 | `primary_key_exclusion_search_non_key_conditions` | 4 | 0.031 | 0.104 | 3.29x | 3.96x (#0) | 0.120 | 3.81x | 4.64x (#0) |
| 62 | `array_uniq_decimal` (`Memory`) | 10 | 0.081 | 0.268 | 3.43x | 5.02x (#8) | 0.280 | 3.65x | 5.09x (#8) |
| 63 | `prewhere_combine_filters` | 3 | 0.308 | 0.903 | 3.41x | 7.90x (#0) | 0.957 | 3.61x | 8.44x (#0) |
| 64 | `read_in_order_many_parts` | 8 | 1.25 | 3.17 | 3.22x | 22x (#0) | 3.39 | 3.60x | 22x (#0) |
| 65 | `non_function_col_short_circuit` (`Memory`) | 4 | 0.022 | 0.064 | 2.97x | 3.18x (#0) | 0.075 | 3.49x | 3.65x (#0) |
| 66 | `aggregate_function_for_each` (`Memory`) | 7 | 0.245 | 0.457 | 3.21x | 5.83x (#0) | 0.477 | 3.42x | 6.07x (#0) |
| 67 | `has_all` | 6 | 0.662 | 0.661 | 2.12x | 8.15x (#13) | 0.369 | 3.40x | 9.92x (#13) |
| 68 | `push_down_volume_reducing_functions` | 9 | 0.591 | 1.59 | 2.71x | 3.96x (#7) | 1.96 | 3.34x | 4.85x (#7) |
| 69 | `merge_tree_insert` | 5 | 0.933 | 0.791 | 1.38x | 4.02x (#0) | 1.48 | 3.18x | 6.63x (#1) |
| 70 | `bloom_filter_select` | 2 | 0.011 | 0.031 | 2.68x | 2.75x (#0) | 0.036 | 3.16x | 3.25x (#0) |
| 71 | `map_bucketed_serialization` | 9 | 0.719 | 2.38 | 3.15x | 11x (#1) | 2.37 | 3.15x | 11x (#7) |
| 72 | `negative_limit_by_in_order` | 9 | 1.82 | 2.38 | 3.02x | 6.92x (#6) | 2.32 | 2.81x | 5.13x (#6) |
| 73 | `text_index_like` | 8 | 0.091 | 0.335 | 2.51x | 19x (#1) | 0.374 | 2.99x | 21x (#1) |
| 74 | `lz4` | 3 | 0.059 | 0.156 | 2.39x | 4.14x (#2) | 0.192 | 2.97x | 4.70x (#2) |
| 75 | `low_cardinality_bulk_insert` (`Memory`) | 1 | 0.109 | 0.320 | 2.94x | 2.94x (#0) | 0.321 | 2.94x | 2.94x (#0) |
| 76 | `merge_table_large_query` | 1 | 0.966 | 2.55 | 2.64x | 2.64x (#0) | 2.84 | 2.94x | 2.94x (#0) |
| 77 | `distinct_partitions` | 6 | 0.385 | 1.67 | 2.28x | 18x (#16) | 1.31 | 2.73x | 17x (#18) |
| 78 | `minmax_count_projection_plan_time` | 5 | 0.027 | 0.043 | 1.61x | 1.68x (#2) | 0.073 | 2.71x | 2.91x (#3) |
| 79 | `short_query_aggregation_overhead` | 1 | 0.119 | 0.208 | 1.74x | 1.74x (#0) | 0.322 | 2.70x | 2.70x (#0) |
| 80 | `top_k_through_array_join` | 10 | 0.300 | 0.622 | 2.47x | 3.47x (#1) | 0.640 | 2.63x | 3.84x (#3) |
| 81 | `enum_string_operation` | 8 | 2.58 | 6.00 | 2.37x | 3.38x (#14) | 3.96 | 2.63x | 3.37x (#14) |
| 82 | `add_distinct_to_in_clause` | 2 | 2.98 | 4.24 | 2.25x | 4.12x (#1) | 4.55 | 2.60x | 5.45x (#1) |
| 83 | `insert_statistics` | 7 | 1.25 | 1.25 | 1.43x | 5.01x (#6) | 2.24 | 2.58x | 10x (#6) |
| 84 | `string_subcolumn_in_tuple` (`Memory`) | 2 | 0.010 | 0.020 | 1.97x | 2.02x (#1) | 0.026 | 2.56x | 2.64x (#1) |
| 85 | `decimal_casts` (`Memory`) | 6 | 0.982 | 1.86 | 2.54x | 4.30x (#0) | 3.15 | 2.31x | 3.35x (#14) |
| 86 | `parse_illegal_datetime` (`Memory`) | 4 | 0.009 | 0.019 | 1.99x | 2.15x (#1) | 0.023 | 2.47x | 2.70x (#1) |
| 87 | `casts` (`Memory`) | 3 | 0.454 | 0.970 | 2.13x | 2.18x (#0) | 1.11 | 2.44x | 2.47x (#1) |
| 88 | `optimize_time_filter_with_preimage` | 4 | 0.450 | 1.13 | 2.10x | 4.60x (#0) | 1.25 | 2.40x | 4.97x (#0) |
| 89 | `sorted_distinct` | 8 | 0.265 | 0.346 | 2.38x | 4.74x (#5) | 0.402 | 2.24x | 4.06x (#5) |
| 90 | `join_convert_full_to_left_or_right` | 1 | 0.008 | 0.015 | 1.97x | 1.97x (#0) | 0.018 | 2.33x | 2.33x (#0) |
| 91 | `array_enumerate_fixed_string` (`Memory`) | 2 | 0.034 | 0.079 | 2.31x | 2.38x (#0) | 0.078 | 2.28x | 2.33x (#0) |
| 92 | `insert_sparse_column` | 1 | 0.133 | 0.281 | 2.11x | 2.11x (#0) | 0.307 | 2.30x | 2.30x (#0) |
| 93 | `lower_upper_utf8_mixed_ascii` (`Memory`) | 4 | 0.024 | 0.040 | 1.89x | 2.91x (#13) | 0.056 | 2.30x | 2.78x (#1) |
| 94 | `has_any_has_all_nullable` | 4 | 0.591 | 0.401 | 2.30x | 17x (#24) | 0.487 | 1.42x | 18x (#24) |
| 95 | `top_k_through_join` | 3 | 0.153 | 0.214 | 1.76x | 3.78x (#0) | 0.265 | 2.27x | 6.41x (#0) |
| 96 | `like_perfect_affix_rewrite` | 2 | 0.190 | 0.409 | 2.16x | 2.16x (#0) | 0.430 | 2.27x | 2.27x (#0) |
| 97 | `sparse_column_index_sorted` | 2 | 0.906 | 2.17 | 2.23x | 2.56x (#1) | 2.17 | 2.25x | 2.55x (#1) |
| 98 | `sort` (`Memory`) | 10 | 1.54 | 2.61 | 2.20x | 49x (#0) | 2.65 | 2.25x | 50x (#0) |
| 99 | `inverse_dictionary_lookup` | 5 | 0.024 | 0.044 | 1.87x | 1.98x (#13) | 0.053 | 2.24x | 2.36x (#13) |
| 100 | `key_condition_multi_atom_wide_or` | 1 | 0.021 | 0.043 | 2.02x | 2.02x (#0) | 0.048 | 2.23x | 2.23x (#0) |
| 101 | `joins_in_memory` (`Memory`) | 5 | 0.153 | 0.188 | 1.23x | 1.29x (#10) | 0.237 | 2.22x | 3.91x (#2) |
| 102 | `has_any_constant_string_needles` | 8 | 2.44 | 3.29 | 2.10x | 14x (#2) | 3.33 | 2.17x | 14x (#2) |
| 103 | `join_merge_filter_into_join` | 2 | 0.012 | 0.021 | 1.74x | 1.75x (#0) | 0.025 | 2.10x | 2.16x (#0) |
| 104 | `low_cardinality_query` | 2 | 0.018 | 0.032 | 1.77x | 1.79x (#1) | 0.038 | 2.09x | 2.12x (#1) |
| 105 | `join_filter_pushdown` | 1 | 0.008 | 0.014 | 1.77x | 1.77x (#0) | 0.016 | 2.08x | 2.08x (#0) |
| 106 | `text_index_low_cardinality_build` | 6 | 1.18 | 0.808 | 0.97x | 2.63x (#0) | 1.71 | 2.07x | 6.37x (#0) |
| 107 | `array_compact` (`Memory`) | 5 | 0.290 | 0.437 | 2.05x | 3.44x (#2) | 0.436 | 2.05x | 3.45x (#2) |
| 108 | `get_map_value` (`Memory`) | 3 | 0.337 | 0.669 | 2.04x | 2.75x (#0) | 0.662 | 2.02x | 2.75x (#0) |
| 109 | `map_subcolumns_prewhere` | 9 | 2.13 | 4.54 | 2.02x | 3.34x (#4) | 4.56 | 2.03x | 3.27x (#4) |
| 110 | `tokenizers` (`Memory`) | 8 | 0.636 | 1.13 | 2.01x | 4.27x (#7) | 1.14 | 2.03x | 4.62x (#2) |
| 111 | `low_cardinality_argument` | 4 | 0.030 | 0.051 | 1.69x | 1.91x (#1) | 0.059 | 1.99x | 2.26x (#1) |
| 112 | `decimal_aggregates` (`Memory`) | 4 | 0.217 | 0.457 | 1.72x | 2.76x (#22) | 0.983 | 1.98x | 8.73x (#3) |
| 113 | `group_by_sundy_li` | 3 | 0.090 | 0.137 | 1.62x | 1.84x (#0) | 0.164 | 1.98x | 2.48x (#0) |
| 114 | `set_index_insert_fixed_keys` | 2 | 0.479 | 0.597 | 1.25x | 1.33x (#1) | 0.945 | 1.98x | 2.12x (#1) |
| 115 | `date_time_64` | 7 | 1.14 | 1.53 | 1.46x | 1.99x (#15) | 2.64 | 1.97x | 4.37x (#10) |
| 116 | `aggregation_in_order_2` | 9 | 2.22 | 2.77 | 1.71x | 5.52x (#0) | 3.03 | 1.93x | 6.91x (#0) |
| 117 | `group_bitwise_if` (`Memory`) | 7 | 0.431 | 0.730 | 1.92x | 13x (#12) | 0.651 | 1.65x | 21x (#12) |
| 118 | `parallel_full_sorting_merge_join` | 8 | 1.30 | 2.08 | 1.75x | 6.32x (#4) | 2.19 | 1.90x | 6.31x (#4) |
| 119 | `logical_functions_small` (`Memory`) | 5 | 0.882 | 2.27 | 1.88x | 2.87x (#3) | 1.43 | 1.66x | 2.79x (#0) |
| 120 | `codecs_float_select` | 6 | 0.669 | 0.944 | 1.43x | 1.81x (#7) | 1.76 | 1.88x | 3.01x (#5) |
| 121 | `join_lazy_indexing` | 2 | 2.69 | 2.88 | 1.10x | 1.19x (#31) | 0.524 | 1.87x | 1.87x (#9) |
| 122 | `aggregation_single_level_result_fanout` | 3 | 0.508 | 0.928 | 1.83x | 1.87x (#0) | 0.859 | 1.69x | 1.74x (#0) |
| 123 | `creating_set_partitions` | 4 | 3.03 | 3.53 | 1.82x | 7.75x (#8) | 1.87 | 1.61x | 3.01x (#2) |
| 124 | `text_index_analysis` | 8 | 0.121 | 0.095 | 1.37x | 2.03x (#4) | 0.121 | 1.76x | 2.50x (#6) |
| 125 | `lot_of_subcolumns` | 1 | 0.038 | 0.066 | 1.70x | 1.70x (#0) | 0.068 | 1.75x | 1.75x (#0) |
| 126 | `join_convert_outer_to_inner_transitively` | 2 | 0.344 | 0.491 | 1.43x | 1.44x (#0) | 0.593 | 1.72x | 1.73x (#1) |
| 127 | `merge_tree_simple_select` | 1 | 0.001 | 0.002 | 1.71x | 1.71x (#0) |  |  |  |
| 128 | `low_cardinality_insert` | 1 | 0.162 | 0.146 | 0.90x | 0.90x (#0) | 0.277 | 1.71x | 1.71x (#0) |
| 129 | `map_lowcardinality_key_lookup` | 5 | 0.764 | 1.20 | 1.64x | 2.35x (#4) | 1.25 | 1.71x | 2.47x (#4) |
| 130 | `set_index_check_in_range` | 1 | 0.234 | 0.392 | 1.67x | 1.67x (#0) | 0.395 | 1.69x | 1.69x (#0) |
| 131 | `text_index_count_stream_resolution` | 2 | 0.042 | 0.062 | 1.47x | 1.57x (#0) | 0.071 | 1.68x | 1.82x (#0) |
| 132 | `narrow_column_read_stream_limit` | 2 | 0.029 | 0.041 | 1.42x | 1.48x (#0) | 0.048 | 1.67x | 1.73x (#0) |
| 133 | `distinct_in_order_final_prefix` | 1 | 0.214 | 0.354 | 1.66x | 1.66x (#0) | 0.321 | 1.50x | 1.50x (#0) |
| 134 | `point_in_polygon_index` | 1 | 0.005 | 0.007 | 1.44x | 1.44x (#0) | 0.009 | 1.65x | 1.65x (#0) |
| 135 | `group_by_multi_low_cardinality` | 4 | 0.429 | 0.506 | 1.58x | 4.71x (#2) | 0.507 | 1.64x | 5.23x (#2) |
| 136 | `trim_whitespace` | 6 | 0.761 | 1.10 | 1.45x | 1.65x (#0) | 1.21 | 1.64x | 2.00x (#3) |
| 137 | `skip_index_order_after_partition_pruning` | 2 | 0.009 | 0.011 | 1.26x | 1.26x (#0) | 0.015 | 1.63x | 1.65x (#0) |
| 138 | `join_used_flags` (`Memory`) | 2 | 0.206 | 0.303 | 1.49x | 1.53x (#0) | 0.315 | 1.63x | 1.77x (#0) |
| 139 | `window_partitions` | 7 | 1.05 | 1.53 | 1.52x | 2.36x (#11) | 1.60 | 1.63x | 2.69x (#11) |
| 140 | `distinct_low_cardinality_prelimit_distinct_in_order` | 6 | 0.334 | 0.235 | 1.59x | 2.84x (#4) | 0.231 | 1.62x | 3.05x (#0) |
| 141 | `count` | 3 | 0.079 | 0.104 | 1.48x | 1.90x (#0) | 0.099 | 1.62x | 2.80x (#0) |
| 142 | `array_distinct` (`Memory`) | 3 | 0.253 | 0.368 | 1.54x | 2.37x (#1) | 0.371 | 1.55x | 2.35x (#1) |
| 143 | `query_interpretation_wide_join` | 7 | 1.85 | 2.50 | 1.39x | 1.68x (#4) | 2.75 | 1.55x | 1.88x (#4) |
| 144 | `group_by_no_aggregates_prefetch` (`Memory`) | 3 | 2.87 | 3.75 | 1.43x | 1.70x (#1) | 4.08 | 1.54x | 1.81x (#1) |
| 145 | `parquet_write_bloom_filter` (`Memory`) | 2 | 0.097 | 0.140 | 1.45x | 1.49x (#0) | 0.148 | 1.54x | 1.58x (#0) |
| 146 | `array_distinct_decimal` (`Memory`) | 8 | 0.231 | 0.296 | 1.48x | 2.40x (#5) | 0.304 | 1.52x | 2.60x (#5) |
| 147 | `logical_functions_medium` (`Memory`) | 4 | 0.288 | 0.535 | 1.52x | 2.80x (#18) | 0.378 | 1.28x | 2.76x (#18) |
| 148 | `minmax_index_insert` | 1 | 0.355 | 0.351 | 0.99x | 0.99x (#0) | 0.537 | 1.51x | 1.51x (#0) |
| 149 | `distinct_low_cardinality` | 2 | 0.034 | 0.038 | 1.36x | 2.36x (#1) | 0.042 | 1.50x | 2.51x (#1) |
| 150 | `set_index_analysis` | 1 | 0.007 | 0.009 | 1.20x | 1.20x (#0) | 0.011 | 1.47x | 1.47x (#0) |
| 151 | `join_key_only_hash_tables` (`Memory`) | 10 | 1.67 | 2.28 | 1.46x | 1.91x (#6) | 2.20 | 1.44x | 1.99x (#1) |
| 152 | `array_join` | 8 | 1.81 | 2.36 | 1.43x | 13x (#11) | 1.81 | 1.30x | 13x (#11) |
| 153 | `array_join_filter_fusion` | 7 | 0.716 | 0.973 | 1.41x | 1.91x (#6) | 0.982 | 1.42x | 1.90x (#6) |
| 154 | `bloom_filter_insert_low_cardinality` | 2 | 1.12 | 1.02 | 1.23x | 2.42x (#0) | 1.15 | 1.41x | 2.98x (#0) |
| 155 | `aggregation_by_partitions` | 7 | 0.149 | 0.117 | 1.36x | 2.53x (#9) | 0.140 | 1.38x | 2.75x (#9) |
| 156 | `join_lazy_replication_sort` | 3 | 0.844 | 1.13 | 1.34x | 1.74x (#2) | 1.12 | 1.36x | 1.81x (#2) |
| 157 | `aggregation_low_cardinality` | 2 | 0.067 | 0.085 | 1.28x | 1.33x (#0) | 0.090 | 1.36x | 1.44x (#0) |
| 158 | `hashjoin_packed_keys` (`Memory`) | 4 | 1.42 | 1.95 | 1.35x | 1.80x (#1) | 1.91 | 1.34x | 1.90x (#1) |
| 159 | `simple_join_query` | 4 | 0.162 | 0.189 | 1.16x | 1.18x (#0) | 0.218 | 1.35x | 1.37x (#3) |
| 160 | `statistics_simple_if` (`Memory`) | 8 | 1.26 | 1.26 | 1.00x | 1.00x (#1) | 1.47 | 1.34x | 10x (#11) |
| 161 | `all_join_opt` | 7 | 0.271 | 0.330 | 1.21x | 1.47x (#0) | 0.361 | 1.33x | 1.61x (#2) |
| 162 | `IPv4` | 4 | 0.120 | 0.154 | 1.29x | 1.32x (#3) | 0.158 | 1.33x | 1.39x (#0) |
| 163 | `extract_key_value_pairs` (`Memory`) | 10 | 1.57 | 1.95 | 1.32x | 1.60x (#2) | 1.94 | 1.32x | 1.60x (#2) |
| 164 | `group_by_no_aggregates_mapped_keys` (`Memory`) | 3 | 0.685 | 0.782 | 1.12x | 1.35x (#2) | 0.902 | 1.31x | 1.48x (#2) |
| 165 | `partial_merge_join` | 4 | 0.115 | 0.135 | 1.16x | 1.73x (#0) | 0.144 | 1.30x | 1.72x (#0) |
| 166 | `inserts_arrays_lowcardinality` | 2 | 0.640 | 0.335 | 0.64x | 1.25x (#0) | 0.674 | 1.29x | 2.52x (#0) |
| 167 | `group_by_low_cardinality_short_strings` | 4 | 0.174 | 0.208 | 1.20x | 1.24x (#0) | 0.222 | 1.28x | 1.33x (#0) |
| 168 | `timeseries_to_grid_bucketing` | 7 | 1.50 | 1.78 | 1.25x | 1.70x (#0) | 1.79 | 1.27x | 1.71x (#0) |
| 169 | `preliminary_distinct_abandoning` | 2 | 0.377 | 0.417 | 1.22x | 1.60x (#0) | 0.435 | 1.27x | 1.69x (#0) |
| 170 | `array_bulk_insert` (`Memory`) | 1 | 0.081 | 0.102 | 1.27x | 1.27x (#0) | 0.100 | 1.23x | 1.23x (#0) |
| 171 | `hashed_dictionary_sharded` (`Memory`) | 5 | 1.78 | 1.99 | 1.25x | 2.23x (#8) | 1.28 | 0.95x | 0.98x (#18) |
| 172 | `parameterized_view_analysis` | 2 | 0.381 | 0.421 | 1.15x | 1.23x (#1) | 0.449 | 1.24x | 1.38x (#1) |
| 173 | `array_join_function_lowering` | 3 | 0.285 | 0.292 | 1.23x | 1.87x (#2) | 0.283 | 1.23x | 2.04x (#2) |
| 174 | `set_in_clustered_keys` | 4 | 1.90 | 1.99 | 1.12x | 1.36x (#2) | 2.04 | 1.23x | 1.64x (#2) |
| 175 | `countIf` | 4 | 0.153 | 0.165 | 1.19x | 2.00x (#3) | 0.166 | 1.22x | 2.24x (#3) |
| 176 | `dict_get_keys_vector_path` | 2 | 1.89 | 2.32 | 1.22x | 1.25x (#1) | 1.85 | 0.98x | 0.98x (#1) |
| 177 | `sparse_column_index_runs` | 6 | 0.520 | 0.586 | 1.20x | 1.60x (#5) | 0.582 | 1.20x | 1.62x (#5) |
| 178 | `text_index_pattern_bypass_cache` | 2 | 0.021 | 0.021 | 1.03x | 1.09x (#0) | 0.025 | 1.20x | 1.28x (#0) |
| 179 | `reverse_string` (`Memory`) | 1 | 0.031 | 0.036 | 1.17x | 1.17x (#0) | 0.037 | 1.20x | 1.20x (#0) |
| 180 | `group_by_string_key_count` (`Memory`) | 1 | 0.389 | 0.463 | 1.19x | 1.19x (#0) | 0.450 | 1.16x | 1.16x (#0) |
| 181 | `aggregate_with_serialized_method` (`Memory`) | 5 | 2.18 | 2.59 | 1.19x | 1.23x (#3) | 2.30 | 1.06x | 1.08x (#1) |
| 182 | `join_squashing_blocks` | 3 | 0.283 | 0.321 | 1.13x | 1.25x (#2) | 0.336 | 1.18x | 1.30x (#2) |
| 183 | `order_by_single_nullable` (`Memory`) | 2 | 2.52 | 2.87 | 1.16x | 1.26x (#0) | 2.92 | 1.18x | 1.29x (#0) |
| 184 | `bloom_filter_insert` | 1 | 0.188 | 0.162 | 0.86x | 0.86x (#0) | 0.222 | 1.18x | 1.18x (#0) |
| 185 | `json_string_value_formatting` (`Memory`) | 6 | 2.19 | 2.43 | 1.15x | 1.27x (#2) | 2.45 | 1.16x | 1.27x (#2) |
| 186 | `dict_join` | 2 | 0.079 | 0.086 | 1.16x | 1.31x (#0) | 0.082 | 1.16x | 1.48x (#0) |
| 187 | `string_order_by` (`Memory`) | 6 | 4.41 | 4.92 | 1.11x | 1.20x (#4) | 5.11 | 1.16x | 1.29x (#5) |
| 188 | `group_by_fixed_keys` | 8 | 0.818 | 0.871 | 1.11x | 2.06x (#7) | 0.950 | 1.16x | 2.31x (#7) |
| 189 | `hashed_dictionary_load_factor` (`Memory`) | 4 | 1.04 | 1.25 | 1.15x | 1.22x (#5) | 3.52 | 1.12x | 1.18x (#4) |
| 190 | `hierarchical_dictionaries` | 10 | 3.01 | 3.34 | 1.15x | 1.33x (#6) | 3.02 | 1.00x | 1.05x (#6) |
| 191 | `parallel_hash_join_various` (`Memory`) | 6 | 2.37 | 2.39 | 1.11x | 1.73x (#5) | 2.41 | 1.14x | 1.85x (#5) |
| 192 | `sort_low_cardinality` | 8 | 1.56 | 1.48 | 1.10x | 2.21x (#0) | 1.51 | 1.13x | 2.30x (#0) |
| 193 | `join_any_build_duplicate_keys` (`Memory`) | 3 | 0.558 | 0.631 | 1.13x | 1.15x (#1) | 0.623 | 1.11x | 1.13x (#1) |
| 194 | `formats_columns_nullable` | 1 | 0.044 | 0.049 | 1.12x | 1.12x (#68) | 0.048 | 1.09x | 1.09x (#68) |
| 195 | `window_functions_sliding_frames` | 7 | 7.83 | 9.12 | 1.12x | 1.87x (#7) | 6.99 | 1.05x | 1.10x (#9) |
| 196 | `sort_wide_table` | 4 | 1.70 | 1.64 | 1.00x | 1.13x (#3) | 1.79 | 1.12x | 1.26x (#3) |
| 197 | `join_convert_to_fixed_hash_table` | 2 | 0.171 | 0.166 | 0.98x | 1.03x (#32) | 0.087 | 1.11x | 1.11x (#35) |
| 198 | `hash_table_iteration_prefetch` (`Memory`) | 1 | 0.678 | 0.736 | 1.09x | 1.09x (#0) | 0.730 | 1.08x | 1.08x (#0) |
| 199 | `wide_table_nested_select` | 3 | 0.513 | 0.546 | 1.08x | 1.13x (#0) | 0.544 | 1.09x | 1.16x (#0) |
| 200 | `window_functions_qualify` | 5 | 3.42 | 3.68 | 1.08x | 1.13x (#4) | 3.69 | 1.08x | 1.13x (#4) |
| 201 | `sort_multi_column_mixed` | 6 | 1.85 | 1.86 | 1.04x | 1.18x (#1) | 1.88 | 1.06x | 1.22x (#5) |
| 202 | `right_full_hash_join` | 8 | 1.55 | 1.64 | 1.06x | 1.09x (#3) | 1.60 | 1.04x | 1.09x (#1) |
| 203 | `sort_nearly_sorted` | 2 | 0.508 | 0.479 | 1.02x | 1.17x (#1) | 0.492 | 1.06x | 1.23x (#1) |
| 204 | `window_functions_many_small_partitions` | 7 | 5.90 | 6.21 | 1.05x | 1.09x (#3) | 6.26 | 1.06x | 1.10x (#3) |
| 205 | `json_key_formatting` (`Memory`) | 4 | 2.45 | 2.54 | 1.04x | 1.08x (#3) | 2.58 | 1.06x | 1.10x (#3) |
| 206 | `hashjoin_with_large_output` (`Memory`) | 3 | 0.636 | 0.671 | 1.06x | 1.06x (#1) | 0.658 | 1.03x | 1.04x (#2) |
| 207 | `uniq_with_key_dist` | 8 | 1.23 | 1.27 | 1.05x | 1.19x (#2) | 1.23 | 1.01x | 1.12x (#2) |
| 208 | `memory_prewhere` (`Memory`) | 5 | 0.074 | 0.048 | 0.74x | 1.57x (#4) | 0.066 | 1.05x | 2.43x (#4) |
| 209 | `dict_get_keys_const_path` | 4 | 0.546 | 0.575 | 1.05x | 1.11x (#1) | 0.564 | 1.03x | 1.11x (#1) |
| 210 | `window_functions_ranking` | 9 | 7.59 | 7.89 | 1.04x | 1.08x (#0) | 7.99 | 1.04x | 1.08x (#3) |
| 211 | `flat_dictionary` (`Memory`) | 9 | 1.34 | 1.39 | 1.03x | 1.05x (#7) | 1.36 | 1.01x | 1.04x (#0) |
| 212 | `jit_sort` | 9 | 6.37 | 6.54 | 1.03x | 1.06x (#8) | 8.54 | 1.03x | 1.09x (#0) |
| 213 | `joined_block_split_single_row` | 3 | 0.146 | 0.146 | 1.00x | 1.02x (#0) | 0.150 | 1.03x | 1.05x (#2) |
| 214 | `direct_dictionary` (`Memory`) | 4 | 2.65 | 2.70 | 1.02x | 1.03x (#13) | 1.67 | 1.00x | 1.03x (#7) |
| 215 | `point_in_polygon_3d_huge_multipolygon` | 1 | 0.034 | 0.031 | 0.93x | 0.93x (#0) | 0.034 | 1.01x | 1.01x (#0) |
| 216 | `subqueries` | 2 | 0.409 | 0.411 | 1.01x | 1.01x (#1) | 0.414 | 1.01x | 1.01x (#1) |
| 217 | `sort_string_full` | 4 | 2.58 | 2.47 | 1.00x | 1.09x (#1) | 2.48 | 1.01x | 1.13x (#1) |
| 218 | `uniq_with_key` | 8 | 2.80 | 2.82 | 1.01x | 1.32x (#4) | 2.71 | 0.97x | 1.02x (#2) |
| 219 | `sum` (`Memory`) | 7 | 0.863 | 0.868 | 1.01x | 1.02x (#5) | 0.680 | 1.00x | 1.01x (#12) |
| 220 | `uniq_without_key` | 3 | 0.372 | 0.356 | 0.79x | 1.07x (#25) | 0.418 | 1.00x | 1.15x (#16) |
| 221 | `limit_by_with_order` | 7 | 3.66 | 3.69 | 0.99x | 1.01x (#19) | 3.87 | 0.77x | 1.00x (#20) |
| 222 | `right_anti_join_with_residual_filter` | 4 | 0.927 | 0.878 | 0.95x | 0.97x (#3) | 0.908 | 0.98x | 1.03x (#3) |
| 223 | `IPv6` | 2 | 0.302 | 0.293 | 0.96x | 0.98x (#1) | 0.296 | 0.98x | 0.98x (#1) |
| 224 | `hashed_array_dictionary` (`Memory`) | 5 | 0.268 | 0.264 | 0.97x | 0.99x (#13) | 0.570 | 0.86x | 0.95x (#21) |
| 225 | `hashed_dictionary` (`Memory`) | 8 | 1.06 | 1.03 | 0.97x | 0.99x (#8) | 1.08 | 0.96x | 0.99x (#2) |
| 226 | `correlated_exists_many_keys` | 1 | 0.525 | 0.506 | 0.96x | 0.96x (#0) | 0.474 | 0.90x | 0.90x (#0) |
| 227 | `native_arrow_ipc` (`Memory`) | 2 | 1.64 | 1.59 | 0.96x | 1.20x (#0) | 1.58 | 0.94x | 1.28x (#0) |
| 228 | `group_by_multiple_strings` | 4 | 0.207 | 0.147 | 0.73x | 0.77x (#1) | 0.464 | 0.96x | 1.83x (#4) |
| 229 | `left_array_join_long_arrays` | 6 | 1.86 | 1.67 | 0.93x | 1.13x (#5) | 1.70 | 0.95x | 1.07x (#5) |
| 230 | `window_functions_downstream_multithreading` | 6 | 3.63 | 3.46 | 0.95x | 1.13x (#5) | 3.41 | 0.94x | 1.15x (#5) |
| 231 | `hash_join_with_row_store` | 8 | 4.89 | 4.55 | 0.92x | 1.06x (#0) | 6.48 | 0.93x | 1.20x (#0) |
| 232 | `aggregate_partitions_independently_default` | 4 | 0.430 | 0.368 | 0.89x | 0.95x (#2) | 0.381 | 0.92x | 1.01x (#2) |
| 233 | `apply_patch_parts_steps` | 4 | 0.622 | 1.60 | 0.80x | 6.03x (#1) | 1.53 | 0.92x | 5.67x (#1) |
| 234 | `aggregation_two_level_bucket_merge` | 4 | 3.66 | 3.15 | 0.90x | 1.14x (#3) | 3.11 | 0.89x | 1.12x (#3) |
| 235 | `storage_join_direct_join` | 1 | 0.251 | 0.205 | 0.82x | 0.82x (#0) | 0.223 | 0.89x | 0.89x (#0) |
| 236 | `uniq_without_key_dist` | 5 | 1.25 | 1.09 | 0.83x | 0.93x (#4) | 1.15 | 0.88x | 0.97x (#4) |
| 237 | `merge_tree_many_partitions` | 1 | 0.003 | 0.002 | 0.64x | 0.64x (#0) | 0.003 | 0.88x | 0.88x (#0) |
| 238 | `query_condition_cache_topk` | 2 | 0.082 | 0.013 | 0.54x | 2.94x (#0) | 0.020 | 0.85x | 4.38x (#0) |
| 239 | `text_index_build` | 9 | 3.41 | 1.09 | 0.47x | 0.83x (#3) | 1.83 | 0.81x | 1.65x (#3) |
| 240 | `groupby_onekey_nullable` | 9 | 1.84 | 1.34 | 0.71x | 0.88x (#6) | 1.48 | 0.80x | 0.89x (#7) |
| 241 | `projection_index_in_read_pools` | 4 | 0.111 | 0.056 | 0.52x | 0.79x (#3) | 0.099 | 0.80x | 1.22x (#1) |
| 242 | `uniqExact_with_key_parallel_merging` | 4 | 2.46 | 1.82 | 0.77x | 0.84x (#3) | 1.83 | 0.77x | 0.84x (#3) |
| 243 | `range_hashed_dictionary` (`Memory`) | 10 | 2.27 | 1.68 | 0.75x | 0.97x (#4) | 1.35 | 0.61x | 0.93x (#4) |
| 244 | `sparse_column_filter` | 9 | 5.25 | 1.65 | 0.64x | 2.13x (#5) | 1.90 | 0.75x | 2.73x (#5) |
| 245 | `bloom_filter_zstd` | 2 | 0.038 | 0.029 | 0.54x | 1.10x (#1) | 0.045 | 0.72x | 1.81x (#1) |
| 246 | `timeseries_to_grid_scalar_add` | 3 | 0.482 | 0.291 | 0.69x | 0.89x (#2) | 0.301 | 0.71x | 0.92x (#1) |
| 247 | `aggregation_in_order_partial_key` | 10 | 1.45 | 0.832 | 0.64x | 1.35x (#5) | 0.827 | 0.67x | 1.51x (#5) |
| 248 | `permuted_columns_cache` | 1 | 0.633 | 0.279 | 0.44x | 0.44x (#0) | 0.410 | 0.65x | 0.65x (#0) |
| 249 | `set_disable_skip_index` | 1 | 0.032 | 0.018 | 0.58x | 0.58x (#0) | 0.020 | 0.62x | 0.62x (#0) |
| 250 | `subcolumn_deserialization_tuple_map` | 10 | 4.75 | 2.14 | 0.47x | 0.76x (#2) | 2.43 | 0.53x | 0.85x (#2) |
| 251 | `window_function_with_order` | 4 | 1.24 | 0.450 | 0.51x | 1.22x (#1) | 0.453 | 0.51x | 1.22x (#1) |
| 252 | `text_index_postprocessor_filter` | 4 | 4.50 | 1.35 | 0.30x | 0.33x (#2) | 2.15 | 0.48x | 0.54x (#2) |
| 253 | `text_index_like_large_dictionary` | 7 | 3.63 | 1.86 | 0.35x | 1.07x (#5) | 2.01 | 0.41x | 1.14x (#5) |
| 254 | `statistics_plan_time_uniq` | 2 | 0.435 | 0.169 | 0.38x | 0.40x (#1) | 0.165 | 0.37x | 0.39x (#1) |
| 255 | `skip_indexes_in_read_pools` | 4 | 0.204 | 0.053 | 0.26x | 0.33x (#1) | 0.076 | 0.38x | 0.47x (#1) |
| 256 | `prefetch_in_aggregation` | 8 | 14.33 | 2.17 | 0.37x | 1.06x (#10) | 2.31 | 0.23x | 0.99x (#13) |
| 257 | `memory_bound_merging` | 1 | 0.402 | 0.133 | 0.33x | 0.33x (#0) | 0.148 | 0.37x | 0.37x (#0) |
| 258 | `insert_statistics_countmin` | 4 | 2.21 | 0.238 | 0.11x | 0.37x (#3) | 0.805 | 0.37x | 1.22x (#3) |
| 259 | `constant_column_after_filter` | 2 | 0.729 | 0.251 | 0.34x | 0.36x (#0) | 0.267 | 0.36x | 0.39x (#0) |
| 260 | `sparse_grams_index_build` | 5 | 9.86 | 2.69 | 0.25x | 1.09x (#3) | 3.11 | 0.34x | 1.08x (#3) |
| 261 | `top_k_threshold_tracker` | 2 | 0.140 | 0.028 | 0.20x | 0.20x (#1) | 0.069 | 0.25x | 0.25x (#0) |
| 262 | `aggregating_merge_tree_simple_aggregate_function_string` | 1 | 0.225 | 0.037 | 0.16x | 0.16x (#0) | 0.040 | 0.18x | 0.18x (#0) |
| 263 | `parallel_index` | 3 | 0.655 | 0.041 | 0.17x | 1.79x (#2) | 0.039 | 0.067x | 0.11x (#0) |
| 264 | `text_index_build_tokenizers` | 3 | 3.60 | 0.308 | 0.086x | 0.087x (#1) | 0.582 | 0.16x | 0.17x (#1) |
| 265 | `array_index_low_cardinality_strings` | 10 | 6.15 | 0.766 | 0.12x | 0.21x (#0) | 0.820 | 0.13x | 0.23x (#0) |
| 266 | `long_primary_key` | 3 | 0.427 | 0.039 | 0.091x | 0.096x (#1) | 0.045 | 0.11x | 0.11x (#1) |
| 267 | `partition_pruning_trace_formatting` | 1 | 0.094 | 0.004 | 0.042x | 0.042x (#0) | 0.006 | 0.061x | 0.061x (#0) |
| 268 | `alter_select` | 4 | 1.73 | 0.017 | 0.053x | 0.44x (#0) | 0.019 | 0.059x | 0.54x (#0) |
| 269 | `insert_sequential_and_background_merges` | 1 | 2.31 | 0.068 | 0.029x | 0.029x (#0) | 0.077 | 0.033x | 0.033x (#0) |
| 270 | `merge_tree_many_partitions_2` | 1 | 0.184 | 0.004 | 0.024x | 0.024x (#0) | 0.005 | 0.030x | 0.030x (#0) |
| 271 | `merge_tree_small_blocks` | 5 | 3.43 | 0.071 | 0.020x | 0.028x (#0) | 0.077 | 0.022x | 0.028x (#0) |
| 272 | `adaptive_codec_merge` | 3 | 2.69 | 0.008 | 0.003x | 0.003x (#0) | 0.013 | 0.005x | 0.006x (#0) |
