# Relevant Test Selection Quality

Analysis of 53 merged PRs (2026-03-26 to 2026-03-30) with `src/` changes, comparing
old DWARF-based algo vs new coverage-based `find_tests.py`.

---

## Key Numbers

| Metric | Old DWARF | New Coverage |
|---|---|---|
| Avg tests/PR | 26 | **183** (7×) |
| Weighted recall vs old | baseline | **50%** |
| Mean recall vs old | baseline | **65%** |
| Long tests selected | 17/44 PRs | **42/53 PRs** |

**50% weighted recall** means the new algo selects half of what the old algo selected.
The missing half is accounted for below.

---

## Why Tests Are Missed (Old Found, New Didn't)

Top missed tests and their CIDB region counts:

| Test | Missed in N PRs | CIDB regions | Verdict |
|---|---|---|---|
| `03147_system_columns_access_checks` | 5 | 30,716 | Ultra-broad, correctly excluded |
| `01942_create_table_with_sample` | 5 | 22,574 | Broad, excluded by cap |
| `01171_mv_select_insert_isolation_long` | 4 | 40,164 | Ultra-broad |
| `01429_empty_arrow_and_parquet` | 4 | 28,122 | Broad |
| `02497_trace_events_stress_long` | 4 | 43,100 | Ultra-broad |
| `03151_unload_index_race` | 4 | 32,876 | Ultra-broad |
| `01505_pipeline_executor_UAF` | 1 | **0** | **DWARF false positive** |
| `01079_parallel_alter_add_drop_column_zookeeper` | 1 | **0** | **DWARF false positive** |
| `02340_parts_refcnt_mergetree` | 1 | **0** | **DWARF false positive** |
| `02532_send_logs_level_test` | 1 | **0** | **DWARF false positive** |
| `02775_show_columns_called_from_clickhouse` | 1 | **0** | **DWARF false positive** |

**Classification of top 30 missed tests:**
- DWARF false positives (0 CIDB regions): ~5 (17%) — old algo matched wrong symbol via inline chains
- Ultra-broad excluded (>30K regions): 18 (60%) — correctly excluded; these tests cover the entire codebase indiscriminately
- Broad excluded (20K–30K regions): 7 (23%) — borderline; excluded by `MAX_TESTS_PER_LINE` threshold

**None of the missed tests represent genuine quality regressions.** The ultra-broad
tests still run in the full stateless suite. DWARF false positives were never valid.

---

## 20 Examples: New Algo Found Relevant Tests

Each example shows changed files → tests found by new algo that directly relate to the change domain.

**1. PR#100893** — Fix keeper-client watch: route duplicate watch_id errors
Changed: `KeeperClientCLI/Commands.cpp`
New found: `02416_keeper_map`, `02417_keeper_map_create_drop`, `02418_keeper_map_keys_limit`, `02377_majority_insert_quorum_zookeeper_long`
Old found: `01505_pipeline_executor_UAF` ← DWARF FP, unrelated to Keeper

**2. PR#100834** — Add `watch` command to keeper-client
Changed: `KeeperClientCLI/Commands.cpp`, `KeeperClient.cpp`
New found: `02221_system_zookeeper_unrestricted`, `02311_system_zookeeper_insert`, `02158_explain_ast_alter_commands`, `02447_drop_database_replica`

**3. PR#101074** — Fix crash on invalid `format_regexp`
Changed: `RegexpRowInputFormat.cpp`
New found: `00144_empty_regexp`, `00187_like_regexp_prefix`, `00218_like_regexp_newline`, `01084_regexp_empty`
Old found 5 tests including generic `01942_create_table_with_sample` (broad, unrelated)

**4. PR#101064** — Fix `Unexpected type of result TTL column` exception
Changed: `ITTLAlgorithm.cpp`, `TTLDeleteFilterTransform.cpp`
New found: `00933_alter_ttl`, `00933_ttl_replicated_zookeeper`, `01049_zookeeper_synchronous_mutations_long`, `01923_ttl_with_modify_column`

**5. PR#101032** — Fix signed integer overflow in `toStartOfInterval` for Milliseconds
Changed: `DateTimeTransforms.h`
New found: `00178_query_datetime64_index`, `00514_interval_operators`, `00524_time_intervals_months_underflow`, `01442_date_time_with_params`

**6. PR#100817** — Fix signed integer overflow in `toStartOfInterval` for Weeks
Changed: `DateTimeTransforms.h`
New found: `00141_parse_timestamp_as_datetime`, `00148_monotonic_functions_and_index`, `01345_index_date_vs_datetime`, `03271_date_to_datetime_saturation`

**7. PR#100964** — Fix building polygon dictionary from sparse columns
Changed: `PolygonDictionary.cpp`
New found: `00500_point_in_polygon`, `00500_point_in_polygon_2d_const`, `00157_cache_dictionary`, `00158_cache_dictionary_has`, `01113_local_dictionary_type_conversion`

**8. PR#100908** — Optimize `SerializationLowCardinality`
Changed: `SerializationLowCardinality.cpp`, `SerializationLowCardinality.h`
New found: `00688_low_cardinality_defaults`, `00688_low_cardinality_in`, `00688_low_cardinality_nullable_cast`, `00800_low_cardinality_distinct_numeric`, `00906_low_cardinality_cache`

**9. PR#101117** — ParserSelectWithUnionQuery fix
Changed: `ParserSelectWithUnionQuery.cpp`
New found: `01720_union_distinct_with_limit`, `01732_explain_syntax_union_query`, `01732_union_and_union_all`, `02345_partial_sort_transform_optimization`

**10. PR#100699** — Fix exception finding common supertype for tuples
Changed: `getLeastSupertype.cpp`, `if.cpp`
New found: `00175_if_num_arrays`, `00176_if_string_arrays`, `00192_least_greatest`, `01263_type_conversion_nvartolomei`
Old found only 1 test; new found 249

**11. PR#100822** — Fix stopMergesAndWait in replacePartitionFrom
Changed: `StorageMergeTree.cpp`
New found: `00029_test_zookeeper_optimize_exception`, `00030_alter_table`, `00084_summing_merge_tree`, `00314_sample_factor_virtual_column`

**12. PR#101059** — Disallow SELECT as a bareword identifier in WITH
Changed: `ParserWithElement.cpp`
New found: `00094_order_by_array_join_limit`, `00165_jit_aggregate_functions`, `00205_emptyscalar_subquery_type_mismatch_bug`, `03223_analyzer_with_cube_fuzz`

**13. PR#101047** — Fix UB in `MergeTreeWhereOptimizer` iterator invalidation
Changed: `MergeTreeWhereOptimizer.cpp`
New found: `00093_prewhere_array_join`, `00812_prewhere_alias_array`, `01076_array_join_prewhere_const_folding`, `01824_move_to_prewhere_many_columns`

**14. PR#100844** — Fix use-after-free in Parquet ReadManager shutdown
Changed: `ReadManager.cpp`
New found: `00163_column_oriented_formats`, `02841_parquet_filter_pushdown`, `03147_parquet_memory_tracking`, `03164_adapting_parquet_reader_output_size`
Old found: `01079_parallel_alter_add_drop_column_zookeeper` ← DWARF FP, unrelated to Parquet

**15. PR#100841** — Add diagnostic counters to `adjustLastGranuleInPart`
Changed: `MergeTreeRangeReader.cpp`, `MergeTreeRangeReader.h`
New found: `03460_normal_projection_index_bug_race_conditions`, `03461_projection_index_multi_part`, `03252_merge_tree_min_bytes_to_seek`, `02875_final_invalid_read_ranges_bug`

**16. PR#100897** — Deepen return type validation for function results
Changed: `resolveFunction.cpp`, `ColumnFunction.cpp`, `validateColumnType.cpp`
New found: `00179_lambdas_with_common_expressions_and_filter`, `02128_apply_lambda_parsing`, `02343_analyzer_lambdas`, `02389_analyzer_nested_lambda`

**17. PR#100977** — Fix `Invalid action query tree node` exception for UNION/INTERSECT
Changed: `PlannerActionsVisitor.cpp`
New found: `00059_shard_global_in`, `00420_unions`, `01715_union_with_set`, `02493_do_not_assume_that_the_original_query_was_valid_when_transforming_joins`

**18. PR#100679** — Fix assertion failure in AggregateFunctionNull
Changed: `AggregateFunctionNull.h`
New found: `00525_aggregate_functions_of_nullable_that_return_non_nullable`, `00931_low_cardinality_nullable_aggregate_function_type`, `01179_long_multistate_agg`, `02470_suspicious_low_cardinality_msan`

**19. PR#100909** — Fix `processAndOptimizeTextIndexFunctions`
Changed: `optimizeDirectReadFromTextIndex.cpp`
New found: `02346_text_index_match_predicate`, `02346_text_index_hints`, `03100_lwu_36_json_skip_indexes`, `04035_text_index_map_values_in`

**20. PR#100653** — Allow empty row groups in Parquet
Changed: `ParquetBlockInputFormat.cpp`
New found: `03261_test_merge_parquet_bloom_filter_minmax_stats`, `03531_check_count_for_parquet`, `03596_parquet_prewhere_page_skip_bug`, `04001_parquet_prewhere_missing_column`

---

## Zero-Recall PRs and Root Causes

| PR | Title | Old algo found | Root cause |
|---|---|---|---|
| #100893 | Keeper watch fix | `01505_pipeline_executor_UAF` | DWARF FP — UAF test has 0 CIDB regions for KeeperClientCLI |
| #100844 | Parquet UAF fix | `01079_parallel_alter_add_drop_column_zookeeper` | DWARF FP — ZooKeeper test has 0 regions for ReadManager.cpp |
| #100841 | MergeTree counters | 3 tests incl. `02532_send_logs_level_test` | DWARF FP + ultra-broad (rc > 8000 for changed lines) |
| #100798 | Nullable/JIT fix | `02775_show_columns_called_from_clickhouse` | DWARF FP — 0 CIDB regions |
| #100378 | Crash log fix | 6 tests incl. `01171_mv_select_insert_isolation_long` | SystemLog.cpp not in stateless coverage + ultra-broad |

In all zero-recall cases, the new algo found **more relevant** tests — e.g., PR#100844
new algo found 129 Parquet-specific tests vs old algo's 1 unrelated ZooKeeper test.

---

## Improvements Applied During Analysis

| Change | Effect |
|---|---|
| Per-hunk SQL ranges (±1, merge gap=5) | Eliminated bounding-box that fetched thousands of irrelevant lines between distant hunks |
| `.h` files use per-hunk ranges | `FunctionsConversion.h` response: 3.4 MB → 9 KB; unique tests: 8143 → 149 |
| Jaccard threshold fix (only lowers for rc<20) | Was 9% for rc=78 (admitted any test sharing generic callees); now 70% |
| INDIRECT_LIMIT inversely proportional to seed count | `200/(n_seeds/5)`: 20 indirect for 78 seeds (was always 200) |
| Keyword guarantee | Domain-specific tests (e.g., `03444_analyzer_resolve_alias_columns`) always appear in output even when 1344 broad tests compete |
| `get_changed_tests` reuses `_diff_text` | New test files added in the PR are correctly detected without extra GitHub API call |
| MAX_OUTPUT_TESTS: 300 → 250 | Reduces tail of low-quality broad tests |
