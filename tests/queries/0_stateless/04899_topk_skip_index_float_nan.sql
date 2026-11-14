-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: the plan assertions read ReadFromMergeTree's own Indexes output.

-- The `minmax` index ranks and skips granules for ORDER BY ... LIMIT by their stored bounds, compared
-- as Fields, which rank NaN greatest. The bounds leave NaN out unless a whole range is NaN, and an
-- index granule merging an all-NaN range with finite ones stores NaN as its max. That matches
-- ORDER BY only under ASC NULLS LAST, so a float key must use the index there and nowhere else.
-- Each `ON` row must equal its `OFF` row, which runs with every TopK optimization off.

SET max_threads = 1;
SET use_query_condition_cache = 0;
SET query_plan_max_limit_for_top_k_optimization = 100;
SET explain_query_plan_default = 'legacy';
-- The defaults, pinned because the test runner randomizes them.
SET use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

-- Three parts, NaN every 100 rows in the last one.
CREATE TABLE i_f64 (id UInt64, v Float64, INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES i_f64;
INSERT INTO i_f64 SELECT number, toFloat64(number + 100) FROM numbers(0, 1000);
INSERT INTO i_f64 SELECT number, toFloat64(number + 100) FROM numbers(1000, 1000);
INSERT INTO i_f64 SELECT number, if(number % 100 = 0, nan, toFloat64(number + 100)) FROM numbers(2000, 1000);

-- Eight granules holding only NaN, then finite values (issue #116705, case 3).
CREATE TABLE i_lead (id UInt64, v Float64, INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES i_lead;
INSERT INTO i_lead SELECT number, nan FROM numbers(0, 512);
INSERT INTO i_lead SELECT number, toFloat64(number) FROM numbers(512, 9488);

-- The same behind LowCardinality, which only a recursive float check sees.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE i_lc (id UInt64, v LowCardinality(Float64), INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;
SYSTEM STOP MERGES i_lc;
INSERT INTO i_lc SELECT number, nan FROM numbers(0, 512);
INSERT INTO i_lc SELECT number, toFloat64(number) FROM numbers(512, 9488);

-- Granule 4 is all NaN and shares an index granule with granule 5, which holds the largest values
-- (issue #124313). i_min mirrors it with the smallest values there.
CREATE TABLE i_max (id UInt32, v Float64, INDEX idx_v v TYPE minmax GRANULARITY 2)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO i_max SELECT number, multiIf(number < 16, number + 1, number < 20, nan, 1000 + number - 20) FROM numbers(24);
CREATE TABLE i_min (id UInt32, v Float64, INDEX idx_v v TYPE minmax GRANULARITY 2)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO i_min SELECT number, multiIf(number < 16, number + 100, number < 20, nan, number - 19) FROM numbers(24);

-- Without dynamic filtering and index use on data read there is no threshold tracker, so only the
-- planner sees NULLS FIRST.
SELECT 'anf ON ', v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1
    SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'anf ON  dflt', v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1;
SELECT 'anf OFF', v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1
    SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- A granule of NaN alone ranks first under DESC.
SELECT 'lead ON ', v FROM i_lead ORDER BY v DESC LIMIT 3;
SELECT 'lead ON  nodyn', v FROM i_lead ORDER BY v DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0;
SELECT 'lead OFF', v FROM i_lead ORDER BY v DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

SELECT 'lc lead ON ', v FROM i_lc ORDER BY v DESC LIMIT 3;
SELECT 'lc lead OFF', v FROM i_lc ORDER BY v DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'lc anf ON ', v FROM i_lc ORDER BY v ASC NULLS FIRST LIMIT 1
    SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'lc anf OFF', v FROM i_lc ORDER BY v ASC NULLS FIRST LIMIT 1
    SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- A merged NaN max is skipped by a DESC threshold.
SELECT 'max ON ', v FROM i_max ORDER BY v DESC LIMIT 3 SETTINGS max_block_size = 4;
SELECT 'max OFF', v FROM i_max ORDER BY v DESC LIMIT 3 SETTINGS max_block_size = 4, use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- The min is not affected by merging, so ASC NULLS LAST keeps the index.
SELECT 'min ON ', v FROM i_min ORDER BY v LIMIT 3 SETTINGS max_block_size = 4;
SELECT 'min ON  nodyn', v FROM i_min ORDER BY v LIMIT 3 SETTINGS max_block_size = 4, use_top_k_dynamic_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'min OFF', v FROM i_min ORDER BY v LIMIT 3 SETTINGS max_block_size = 4, use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'anl ON ', v FROM i_f64 ORDER BY v LIMIT 3;
SELECT 'anl OFF', v FROM i_f64 ORDER BY v LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;

-- Which reads use the index: buildIndexes decides at the defaults, the planner alone without a
-- threshold tracker.
SELECT 'idx anl', count() > 0 FROM (EXPLAIN indexes = 1 SELECT v FROM i_f64 ORDER BY v LIMIT 3) WHERE explain ILIKE '%topk granules%';
SELECT 'idx anl nodyn', count() > 0 FROM (EXPLAIN indexes = 1 SELECT v FROM i_f64 ORDER BY v LIMIT 3
    SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%topk granules%';
SELECT 'idx min', count() > 0 FROM (EXPLAIN indexes = 1 SELECT v FROM i_min ORDER BY v LIMIT 3) WHERE explain ILIKE '%topk granules%';
SELECT 'idx anf nodyn', count() FROM (EXPLAIN indexes = 1 SELECT v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1
    SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%topk granules%';
SELECT 'idx anf', count() FROM (EXPLAIN indexes = 1 SELECT v FROM i_f64 ORDER BY v ASC NULLS FIRST LIMIT 1) WHERE explain ILIKE '%topk granules%';
SELECT 'idx dnl', count() FROM (EXPLAIN indexes = 1 SELECT v FROM i_lead ORDER BY v DESC LIMIT 3) WHERE explain ILIKE '%topk granules%';

-- A read the planner chose for TopK is kept out of plan-based parallel replicas, so a DESC read,
-- which cannot use the index, must stay distributed while the ASC one stays local. NULLS FIRST,
-- because DESC NULLS LAST is also refused for its nulls direction.
SELECT 'prdist dnf', count() > 0 FROM (EXPLAIN SELECT v FROM i_f64 ORDER BY v DESC NULLS FIRST LIMIT 1
    SETTINGS use_top_k_dynamic_filtering = 0, enable_parallel_replicas = 1,
             parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
             parallel_replicas_local_plan = 1, parallel_replicas_plan_based = 1,
             automatic_parallel_replicas_mode = 0) WHERE explain LIKE '%ReadFromParallelReplicas%';
SELECT 'prdist anl', count() FROM (EXPLAIN SELECT v FROM i_f64 ORDER BY v LIMIT 1
    SETTINGS use_top_k_dynamic_filtering = 0, enable_parallel_replicas = 1,
             parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
             parallel_replicas_local_plan = 1, parallel_replicas_plan_based = 1,
             automatic_parallel_replicas_mode = 0) WHERE explain LIKE '%ReadFromParallelReplicas%';
