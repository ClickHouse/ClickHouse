-- Tags: no-parallel-replicas
-- no-parallel-replicas: replicas execute the subqueries themselves.

-- Per-partition index analysis runs on several threads; an `IN (subquery)` set it needs
-- must be executed once, not once per thread.

DROP TABLE IF EXISTS t_subquery_set_built_once;

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
    max_block_size = 65409, log_comment = '05339_primary_key';

SELECT count() FROM t_subquery_set_built_once
WHERE y < 100000 AND y + p IN (SELECT number FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    max_block_size = 65409, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, log_comment = '05339_skip_index';

SELECT count() FROM t_subquery_set_built_once
WHERE mapContainsKeyValue(m, 'a', '1') AND m[toString(p)] IN (SELECT toString(number) FROM numbers(20) WHERE NOT ignore(sleep(0.1)))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    max_block_size = 65409, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    log_comment = '05339_text_index_map';

-- A set built during planning is still used by the per-partition conditions.
SELECT count() FROM t_subquery_set_built_once
WHERE x IN (SELECT number FROM numbers(10))
SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_projections = 1, max_threads = 8, max_threads_for_indexes = 8,
    log_comment = '05339_planned_set';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SleepFunctionCalls']
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05339_primary_key', '05339_skip_index', '05339_text_index_map') AND type = 'QueryFinish'
ORDER BY log_comment;

SELECT log_comment, read_rows < 500
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05339_planned_set' AND type = 'QueryFinish';

DROP TABLE t_subquery_set_built_once;
