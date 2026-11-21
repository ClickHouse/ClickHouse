-- Tags: no-parallel-replicas
-- no-parallel-replicas: replicas execute the subqueries themselves.

-- Per-partition index analysis runs on several threads; an `IN (subquery)` set it needs
-- must be executed once, not once per thread.

DROP TABLE IF EXISTS t_subquery_set_built_once;
DROP TABLE IF EXISTS t_subquery_set_built_once_statistics;

CREATE TABLE t_subquery_set_built_once
(
    p UInt8,
    x UInt64,
    y UInt64,
    m Map(String, String),
    INDEX iy y TYPE minmax GRANULARITY 1,
    INDEX im m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1,
    PROJECTION pr (SELECT p, x, y, m ORDER BY y)
)
ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS index_granularity = 8;

INSERT INTO t_subquery_set_built_once SELECT number % 8, number, number, map('a', '1', toString(number % 8), toString(number)) FROM numbers(1000);

-- The normal projection makes index analysis run while the query is planned, before the subquery is executed.
SELECT count() FROM t_subquery_set_built_once
WHERE x + p IN (SELECT number FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    max_block_size = 65409, use_query_condition_cache = 0, log_comment = '05339_primary_key';

SELECT count() FROM t_subquery_set_built_once
WHERE y < 100000 AND y + p IN (SELECT number FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    max_block_size = 65409, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0, log_comment = '05339_skip_index';

-- The skip index reader at read time uses the conditions generated while the query is planned.
SELECT count() FROM t_subquery_set_built_once
WHERE y < 100000 AND y + p IN (SELECT number FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    max_block_size = 65409, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, use_query_condition_cache = 0, log_comment = '05339_skip_index_on_data_read';

SELECT count() FROM t_subquery_set_built_once
WHERE mapContainsKeyValue(m, 'a', '1') AND m[toString(p)] IN (SELECT toString(number) FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    max_block_size = 65409, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    log_comment = '05339_text_index_map';

-- With EXPLAIN, the subquery is executed only to analyze the indexes: not when the primary key drops every part.
SET ignore_format_null_for_explain = 0;

EXPLAIN indexes = 1 SELECT count() FROM t_subquery_set_built_once
WHERE x = 99999 AND y < 100000 AND y + p IN (SELECT number FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_statistics_for_part_pruning = 0, optimize_move_to_prewhere = 0,
    use_query_condition_cache = 0, log_comment = '05339_primary_key_drops_all' FORMAT Null;

EXPLAIN indexes = 1 SELECT count() FROM t_subquery_set_built_once
WHERE x = 99999 AND y < 100000 AND y + p IN (SELECT number FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, use_statistics_for_part_pruning = 0, optimize_move_to_prewhere = 0,
    use_query_condition_cache = 0, log_comment = '05339_primary_key_drops_all_on_data_read' FORMAT Null;

-- Neither to describe the conditions of the parts in the per-part statistics.
EXPLAIN indexes = 1 SELECT count() FROM t_subquery_set_built_once
WHERE x = 99999 AND y < 100000 AND y + p IN (SELECT number FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_statistics_for_part_pruning = 0, optimize_move_to_prewhere = 0,
    per_part_index_stats = 1, use_query_condition_cache = 0, log_comment = '05339_primary_key_drops_all_per_part_stats' FORMAT Null;

-- A set built during planning is still used by the per-partition conditions.
SELECT count() FROM t_subquery_set_built_once
WHERE x IN (SELECT number FROM numbers(10))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    log_comment = '05339_planned_set';

-- Deciding whether to load the statistics analyzes the primary key of the parts on several threads.
CREATE TABLE t_subquery_set_built_once_statistics (p UInt8, x UInt64 STATISTICS(basic))
ENGINE = MergeTree PARTITION BY p ORDER BY x
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', use_primary_key_cache = 0, prewarm_statistics_cache = 0;

INSERT INTO t_subquery_set_built_once_statistics SELECT number % 8, number FROM numbers(1000)
SETTINGS materialize_statistics_on_insert = 1;

-- Unloads the primary index and the statistics of the parts.
DETACH TABLE t_subquery_set_built_once_statistics;
ATTACH TABLE t_subquery_set_built_once_statistics;

-- The implicit projection makes index analysis run while the query is planned.
SELECT count() FROM t_subquery_set_built_once_statistics
WHERE x < 100000 AND x + p IN (SELECT number FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1,
    max_threads = 8, max_threads_for_indexes = 8, use_statistics = 1, use_statistics_for_part_pruning = 1,
    min_marks_per_part_to_load_statistics = 2, optimize_move_to_prewhere = 0, use_query_condition_cache = 0,
    log_comment = '05339_statistics';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT log_comment, ProfileEvents['SleepFunctionCalls']
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05339_primary_key', '05339_skip_index', '05339_skip_index_on_data_read', '05339_text_index_map',
    '05339_primary_key_drops_all', '05339_primary_key_drops_all_on_data_read', '05339_primary_key_drops_all_per_part_stats', '05339_statistics')
    AND type = 'QueryFinish'
ORDER BY log_comment;

-- The probe analyzes the parts on several threads, and their per-partition conditions leave too few marks to load the statistics.
SELECT DISTINCT message_format_string
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND log_comment = '05339_statistics' AND type = 'QueryFinish')
    AND message_format_string IN (
        'Analyzing the primary key of parts in {} threads to decide whether to load statistics',
        'Not pruning {} parts without statistics in memory, because fewer than {} marks are left to read in them by the primary key')
ORDER BY message_format_string;

-- The per-partition conditions use the set to prune.
SELECT log_comment, read_rows < 500
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05339_primary_key', '05339_skip_index', '05339_skip_index_on_data_read', '05339_planned_set') AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_subquery_set_built_once;
DROP TABLE t_subquery_set_built_once_statistics;
