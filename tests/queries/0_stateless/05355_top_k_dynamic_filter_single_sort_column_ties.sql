-- Random settings limits: query_plan_max_limit_for_top_k_optimization=(20000, None); use_top_k_dynamic_filtering=(1, None); use_top_k_dynamic_filtering_for_single_string_sort_column=(1, None)

-- With a single `String` `ORDER BY` column, the top-K dynamic filter (`__topKFilter`) also drops the rows equal to the
-- threshold once most rows of a block are equal to it, and from then on the read records nothing in the query condition
-- cache. The sort key values of every result must stay the same as without the filter, also with ties at the end of the
-- `LIMIT`, `OFFSET`, `NULL` and with the query condition cache. With two sort columns, and for `GROUP BY key LIMIT n`,
-- the rows equal to the threshold must be kept.

DROP TABLE IF EXISTS t_topk_ties;
DROP TABLE IF EXISTS t_topk_sparse;
DROP TABLE IF EXISTS t_gb_topk_ties;
DROP TABLE IF EXISTS t_gb_topk_dominant;

CREATE TABLE t_topk_ties (id UInt64, s String, n UInt32, ns Nullable(String), t String, tn Nullable(String))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_topk_ties;

-- `s` is mostly empty and `n` mostly 7. `t` has 50 values of about 400 rows each, scattered over the granules, so that a
-- `LIMIT` ends inside a group of tied rows. `ns` and `tn` are `NULL` in a third of the rows. Three parts.
INSERT INTO t_topk_ties SELECT number, if(number % 20 = 0, concat('m', toString(number % 997)), ''), if(number % 10 = 0, number % 13, 7),
    if(number % 3 = 0, NULL, if(number % 20 = 0, toString(number % 101), '')), leftPad(toString(intDiv(number, 7) % 50), 2, '0'),
    if(number % 3 = 1, NULL, leftPad(toString(intDiv(number, 7) % 50), 2, '0'))
FROM numbers(0, 7000);
INSERT INTO t_topk_ties SELECT number, if(number % 20 = 0, concat('m', toString(number % 997)), ''), if(number % 10 = 0, number % 13, 7),
    if(number % 3 = 0, NULL, if(number % 20 = 0, toString(number % 101), '')), leftPad(toString(intDiv(number, 7) % 50), 2, '0'),
    if(number % 3 = 1, NULL, leftPad(toString(intDiv(number, 7) % 50), 2, '0'))
FROM numbers(7000, 7000);
INSERT INTO t_topk_ties SELECT number, if(number % 20 = 0, concat('m', toString(number % 997)), ''), if(number % 10 = 0, number % 13, 7),
    if(number % 3 = 0, NULL, if(number % 20 = 0, toString(number % 101), '')), leftPad(toString(intDiv(number, 7) % 50), 2, '0'),
    if(number % 3 = 1, NULL, leftPad(toString(intDiv(number, 7) % 50), 2, '0'))
FROM numbers(14000, 6000);

-- Each check prints whether the sort key values of the result, in order (as tuples, which keep `NULL`), are equal to those
-- of the same query without the filter and without the query condition cache, and their number. Other columns of rows
-- tied on the sort key may differ.
SELECT 'string asc', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_ties ORDER BY s LIMIT 30)) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_ties ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'string desc', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_ties ORDER BY s DESC LIMIT 30)) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_ties ORDER BY s DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'string offset', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_ties ORDER BY s DESC LIMIT 900, 50)) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_ties ORDER BY s DESC LIMIT 900, 50) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'string where', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_ties WHERE s != '' ORDER BY s LIMIT 30)) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_ties WHERE s != '' ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'ties', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 1000)) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 1000) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'nullable nulls first', a = b, length(a), countEqual(a, tuple(NULL)) FROM (SELECT
    (SELECT groupArray(tuple(ns)) FROM (SELECT ns FROM t_topk_ties ORDER BY ns NULLS FIRST LIMIT 30)) AS a,
    (SELECT groupArray(tuple(ns)) FROM (SELECT ns FROM t_topk_ties ORDER BY ns NULLS FIRST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'nullable desc nulls last, NULL threshold', a = b, length(a), countEqual(a, tuple(NULL)) FROM (SELECT
    (SELECT groupArray(tuple(ns)) FROM (SELECT ns FROM t_topk_ties ORDER BY ns DESC NULLS LAST LIMIT 14000) SETTINGS query_plan_max_limit_for_top_k_optimization = 20000) AS a,
    (SELECT groupArray(tuple(ns)) FROM (SELECT ns FROM t_topk_ties ORDER BY ns DESC NULLS LAST LIMIT 14000) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'nullable ties', a = b, length(a), countEqual(a, tuple(NULL)) FROM (SELECT
    (SELECT groupArray(tuple(tn)) FROM (SELECT tn FROM t_topk_ties ORDER BY tn LIMIT 1000)) AS a,
    (SELECT groupArray(tuple(tn)) FROM (SELECT tn FROM t_topk_ties ORDER BY tn LIMIT 1000) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'with ties', a = b, a FROM (SELECT
    (SELECT count() FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 5 WITH TIES)) AS a,
    (SELECT count() FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 5 WITH TIES) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'distinct', a = b, length(a) FROM (SELECT
    (SELECT groupArray(t) FROM (SELECT DISTINCT t FROM t_topk_ties ORDER BY t LIMIT 10)) AS a,
    (SELECT groupArray(t) FROM (SELECT DISTINCT t FROM t_topk_ties ORDER BY t LIMIT 10) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'limit by', a = b, length(a) FROM (SELECT
    (SELECT groupArray(t) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 2 BY t LIMIT 10)) AS a,
    (SELECT groupArray(t) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 2 BY t LIMIT 10) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
-- With two sort columns, a row equal to the threshold on `s` can still win on `n`.
SELECT 'two columns', a = b, length(a) FROM (SELECT
    (SELECT groupArray((s, n)) FROM (SELECT s, n FROM t_topk_ties ORDER BY s, n DESC LIMIT 30)
        SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1, max_threads = 1, max_block_size = 64) AS a,
    (SELECT groupArray((s, n)) FROM (SELECT s, n FROM t_topk_ties ORDER BY s, n DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'returned rows exist', count() FROM (SELECT id, s, ns FROM t_topk_ties ORDER BY ns NULLS FIRST LIMIT 30)
WHERE (id, s, isNull(ns), coalesce(ns, '')) IN (SELECT id, s, isNull(ns), coalesce(ns, '') FROM t_topk_ties);
-- Without the buffer, `q` is read and sorted twice, and both sorts publish to one threshold.
SELECT 'correlated subquery, unbuffered', count(), sum(n) FROM (
    SELECT s, (SELECT count() FROM numbers(3) WHERE toString(number) >= q.s) AS n
    FROM (SELECT s FROM t_topk_ties ORDER BY s LIMIT 10) AS q)
SETTINGS correlated_subqueries_use_in_memory_buffer = 0, max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0,
    use_query_condition_cache = 0;

-- Sparse columns: `s` and `n` are mostly default values.
CREATE TABLE t_topk_sparse (id UInt64, s String, n UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 0.5, compute_exact_num_defaults_for_sparse_columns = 1;
-- The non-default values alternate between large increasing ones and small ones.
INSERT INTO t_topk_sparse SELECT number, if(number % 20 = 0, if(number % 40 = 0, leftPad(toString(number), 6, '0'), '0'), ''),
    if(number % 20 = 0, if(number % 40 = 0, number, 1), 0)
FROM numbers(20000);
SELECT 'sparse', groupArray(serialization_kind) FROM (SELECT DISTINCT column, serialization_kind FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_topk_sparse' AND active AND column IN ('s', 'n') ORDER BY column);
SELECT 'sparse string', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_sparse ORDER BY s LIMIT 30) SETTINGS max_threads = 1, max_block_size = 64) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_sparse ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'sparse string desc', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_sparse ORDER BY s DESC LIMIT 30) SETTINGS max_threads = 1, max_block_size = 64) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_topk_sparse ORDER BY s DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'sparse number', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(n)) FROM (SELECT n FROM t_topk_sparse ORDER BY n LIMIT 30) SETTINGS max_threads = 1, max_block_size = 64) AS a,
    (SELECT groupArray(tuple(n)) FROM (SELECT n FROM t_topk_sparse ORDER BY n LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'sparse number desc', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(n)) FROM (SELECT n FROM t_topk_sparse ORDER BY n DESC LIMIT 30) SETTINGS max_threads = 1, max_block_size = 64) AS a,
    (SELECT groupArray(tuple(n)) FROM (SELECT n FROM t_topk_sparse ORDER BY n DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);

-- Once the threshold is the empty string, which most rows are equal to, a single sort column (also a `Nullable` one) drops
-- every later row in PREWHERE, so the main reader reads `id` for few rows. With a second sort column, the rows equal to the
-- threshold are kept.
SELECT id, s FROM t_topk_ties ORDER BY s LIMIT 20 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0, use_query_condition_cache = 0,
    use_top_k_dynamic_filtering_for_variable_length_types = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    serialize_query_plan = 0, log_comment = '05355_one_column';
SELECT id, ns FROM t_topk_ties ORDER BY ns LIMIT 20 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0, use_query_condition_cache = 0,
    use_top_k_dynamic_filtering_for_variable_length_types = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    serialize_query_plan = 0, log_comment = '05355_one_column_nullable';
SELECT id, s FROM t_topk_ties ORDER BY s, n LIMIT 20 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0, use_query_condition_cache = 0,
    use_top_k_dynamic_filtering_for_variable_length_types = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    serialize_query_plan = 0, log_comment = '05355_two_columns';

-- The query condition cache records the granules without a row that passes the PREWHERE under a key that includes the
-- top-K plan. Fill it with runs that read the granules in different orders, then check reruns with the same and with
-- changed `LIMIT`, `OFFSET`, direction and `WHERE`.
SELECT t FROM t_topk_ties ORDER BY t LIMIT 600 FORMAT Null
SETTINGS use_query_condition_cache = 1, max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;
SELECT t FROM t_topk_ties ORDER BY t LIMIT 600 FORMAT Null SETTINGS use_query_condition_cache = 1, max_threads = 4, max_block_size = 100;
SELECT t FROM t_topk_ties ORDER BY t DESC LIMIT 600 FORMAT Null SETTINGS use_query_condition_cache = 1, max_threads = 2, max_block_size = 64;
SELECT tn FROM t_topk_ties ORDER BY tn LIMIT 600 FORMAT Null SETTINGS use_query_condition_cache = 1, max_threads = 2, max_block_size = 64;
SELECT t FROM t_topk_ties ORDER BY t LIMIT 600 FORMAT Null
SETTINGS use_query_condition_cache = 1, max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0, log_comment = '05355_cached_rerun';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['RowsReadByMainReader'] < 2000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05355_one_column', '05355_one_column_nullable', '05355_two_columns')
ORDER BY log_comment;
SELECT 'cache read', ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05355_cached_rerun';

-- A read that dropped the rows equal to the threshold records nothing, so a rerun selects the same rows. A read that
-- kept them records its empty granules, so a rerun selects fewer rows. Each pair has its own key (`LIMIT`).
SELECT s FROM t_topk_ties ORDER BY s LIMIT 25 FORMAT Null
SETTINGS use_query_condition_cache = 1, max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0, log_comment = '05355_q1_run1';
SELECT s FROM t_topk_ties ORDER BY s LIMIT 25 FORMAT Null
SETTINGS use_query_condition_cache = 1, max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0, log_comment = '05355_q1_run2';
SELECT t FROM t_topk_ties ORDER BY t LIMIT 40 FORMAT Null
SETTINGS use_query_condition_cache = 1, max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0, log_comment = '05355_q2_run1';
SELECT t FROM t_topk_ties ORDER BY t LIMIT 40 FORMAT Null
SETTINGS use_query_condition_cache = 1, max_threads = 1, max_block_size = 64, query_plan_optimize_lazy_materialization = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0, log_comment = '05355_q2_run2';

SYSTEM FLUSH LOGS query_log;
SELECT 'no cache entries after dropping ties', run2 = run1 FROM (SELECT
    anyIf(ProfileEvents['SelectedRows'], log_comment = '05355_q1_run1') AS run1,
    anyIf(ProfileEvents['SelectedRows'], log_comment = '05355_q1_run2') AS run2
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05355_q1_run1', '05355_q1_run2'));
SELECT 'cache entries with ties kept', run2 < run1, hits > 0 FROM (SELECT
    anyIf(ProfileEvents['SelectedRows'], log_comment = '05355_q2_run1') AS run1,
    anyIf(ProfileEvents['SelectedRows'], log_comment = '05355_q2_run2') AS run2,
    anyIf(ProfileEvents['QueryConditionCacheHits'], log_comment = '05355_q2_run2') AS hits
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05355_q2_run1', '05355_q2_run2'));

SELECT 'cached same plan', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 600) SETTINGS use_query_condition_cache = 1) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 600) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'cached same plan, 4 threads', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 600) SETTINGS use_query_condition_cache = 1, max_threads = 4) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 600) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'cached same LIMIT + OFFSET', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 200 OFFSET 400) SETTINGS use_query_condition_cache = 1) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 200 OFFSET 400) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'cached larger limit', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 1000) SETTINGS use_query_condition_cache = 1) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 1000) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'cached smaller limit', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 300) SETTINGS use_query_condition_cache = 1) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t LIMIT 300) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'cached desc', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t DESC LIMIT 600) SETTINGS use_query_condition_cache = 1) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties ORDER BY t DESC LIMIT 600) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'cached where', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties WHERE t != '00' ORDER BY t LIMIT 600) SETTINGS use_query_condition_cache = 1) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_topk_ties WHERE t != '00' ORDER BY t LIMIT 600) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);
SELECT 'cached nullable', a = b, length(a), countEqual(a, tuple(NULL)) FROM (SELECT
    (SELECT groupArray(tuple(tn)) FROM (SELECT tn FROM t_topk_ties ORDER BY tn LIMIT 600) SETTINGS use_query_condition_cache = 1) AS a,
    (SELECT groupArray(tuple(tn)) FROM (SELECT tn FROM t_topk_ties ORDER BY tn LIMIT 600) SETTINGS use_top_k_dynamic_filtering = 0, use_query_condition_cache = 0) AS b);

-- `GROUP BY k [ORDER BY k] LIMIT n` keeps the rows equal to its boundary: the key on the boundary is in every block of
-- every part, and the later rows of its group still count in `count()` and `sum(v)`.
CREATE TABLE t_gb_topk_ties (id UInt32, k UInt32, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_gb_topk_ties;
INSERT INTO t_gb_topk_ties SELECT number, if(number % 97 = 0, 0, 1 + number % 1000), number FROM numbers(0, 10000);
INSERT INTO t_gb_topk_ties SELECT number, if(number % 97 = 0, 0, 1 + number % 1000), number FROM numbers(10000, 10000);
INSERT INTO t_gb_topk_ties SELECT number, if(number % 97 = 0, 0, 1 + number % 1000), number FROM numbers(20000, 10000);

SELECT 'group by smallest key', a = b, a FROM (SELECT
    (SELECT groupArray((k, c, sv)) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_gb_topk_ties GROUP BY k ORDER BY k LIMIT 1)
        SETTINGS max_threads = 1, max_block_size = 128, use_query_condition_cache = 0, serialize_query_plan = 0, enable_parallel_replicas = 0,
            automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0, enable_group_by_top_k_optimization = 1,
            enable_group_by_top_k_dynamic_filtering = 1, optimize_aggregation_in_order = 0, optimize_trivial_group_by_limit_query = 0) AS a,
    (SELECT groupArray((k, c, sv)) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_gb_topk_ties GROUP BY k ORDER BY k LIMIT 1)
        SETTINGS enable_group_by_top_k_optimization = 0, use_query_condition_cache = 0) AS b);
SELECT 'group by largest key', a = b, a FROM (SELECT
    (SELECT groupArray((k, c, sv)) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_gb_topk_ties GROUP BY k ORDER BY k DESC LIMIT 1)
        SETTINGS max_threads = 1, max_block_size = 128, use_query_condition_cache = 0, serialize_query_plan = 0, enable_parallel_replicas = 0,
            automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0, enable_group_by_top_k_optimization = 1,
            enable_group_by_top_k_dynamic_filtering = 1, optimize_aggregation_in_order = 0, optimize_trivial_group_by_limit_query = 0) AS a,
    (SELECT groupArray((k, c, sv)) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_gb_topk_ties GROUP BY k ORDER BY k DESC LIMIT 1)
        SETTINGS enable_group_by_top_k_optimization = 0, use_query_condition_cache = 0) AS b);
SELECT 'group by three smallest keys', a = b, a FROM (SELECT
    (SELECT groupArray((k, c, sv)) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_gb_topk_ties GROUP BY k ORDER BY k LIMIT 3)
        SETTINGS max_threads = 4, max_block_size = 128, use_query_condition_cache = 0, serialize_query_plan = 0, enable_parallel_replicas = 0,
            automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0, enable_group_by_top_k_optimization = 1,
            enable_group_by_top_k_dynamic_filtering = 1, optimize_aggregation_in_order = 0, optimize_trivial_group_by_limit_query = 0) AS a,
    (SELECT groupArray((k, c, sv)) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_gb_topk_ties GROUP BY k ORDER BY k LIMIT 3)
        SETTINGS enable_group_by_top_k_optimization = 0, use_query_condition_cache = 0) AS b);

-- The key on the boundary is most rows of every block.
CREATE TABLE t_gb_topk_dominant (id UInt32, k UInt32, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_gb_topk_dominant;
INSERT INTO t_gb_topk_dominant SELECT number, if(number % 5 < 3, 0, 1 + number % 1000), number FROM numbers(0, 10000);
INSERT INTO t_gb_topk_dominant SELECT number, if(number % 5 < 3, 0, 1 + number % 1000), number FROM numbers(10000, 10000);
INSERT INTO t_gb_topk_dominant SELECT number, if(number % 5 < 3, 0, 1 + number % 1000), number FROM numbers(20000, 10000);
SELECT 'group by dominating smallest key', a = b, a FROM (SELECT
    (SELECT groupArray((k, c, sv)) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_gb_topk_dominant GROUP BY k ORDER BY k LIMIT 1)
        SETTINGS max_threads = 1, max_block_size = 128, use_query_condition_cache = 0, serialize_query_plan = 0, enable_parallel_replicas = 0,
            automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0, enable_group_by_top_k_optimization = 1,
            enable_group_by_top_k_dynamic_filtering = 1, optimize_aggregation_in_order = 0, optimize_trivial_group_by_limit_query = 0) AS a,
    (SELECT groupArray((k, c, sv)) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_gb_topk_dominant GROUP BY k ORDER BY k LIMIT 1)
        SETTINGS enable_group_by_top_k_optimization = 0, use_query_condition_cache = 0) AS b);

DROP TABLE t_topk_ties;
DROP TABLE t_topk_sparse;
DROP TABLE t_gb_topk_ties;
DROP TABLE t_gb_topk_dominant;
