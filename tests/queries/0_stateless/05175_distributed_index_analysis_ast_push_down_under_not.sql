-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/113445
-- The polarity hole in `tryBuildAdditionalFilterAST` also affected its second caller, `getFilterAST`
-- in `distributedIndexAnalysis.cpp`, which sends the predicate to the replicas that analyze the
-- indexes. A predicate made stronger by dropping a conjunct under `NOT` over-prunes mark ranges
-- there, and no initiator-side re-filter can bring the lost granules back.

DROP TABLE IF EXISTS t_push_ast_index;
CREATE TABLE t_push_ast_index (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY intDiv(a, 125)
SETTINGS index_granularity = 8, distributed_index_analysis_min_parts_to_activate = 0,
    distributed_index_analysis_min_indexes_bytes_to_activate = 0;
-- One part per partition, so that the parts are spread over the replicas and the remote replicas
-- receive the predicate as an AST instead of everything being resolved on the initiator.
SYSTEM STOP MERGES t_push_ast_index;
INSERT INTO t_push_ast_index SELECT number, number % 100 FROM numbers(1000);

SET cluster_for_parallel_replicas = 'parallel_replicas';
SET allow_experimental_parallel_reading_from_replicas = 0;
SET max_parallel_replicas = 4;
SET distributed_index_analysis = 1;
SET distributed_index_analysis_for_non_shared_merge_tree = 1;
-- Ignore warnings when a replica does not respond and the analysis falls back to the initiator.
SET send_logs_level = 'error';

SELECT 'the answer does not change with distributed index analysis';
SELECT count() FROM t_push_ast_index WHERE NOT (a < 500 AND arrayExists(x -> x < 86, [b]))
SETTINGS distributed_index_analysis = 0;
SELECT count() FROM t_push_ast_index WHERE NOT (a < 500 AND arrayExists(x -> x < 86, [b]));

SELECT 'a nested AND under a top-level AND, where dropping a conjunct is still sound';
SELECT count() FROM t_push_ast_index WHERE a < 900 AND (a > 100 AND arrayExists(x -> x < 86, [b]))
SETTINGS distributed_index_analysis = 0;
SELECT count() FROM t_push_ast_index WHERE a < 900 AND (a > 100 AND arrayExists(x -> x < 86, [b]));

-- The wrong answer is not observable through `count` here, because `KeyCondition` cannot exploit a
-- `NOT` over an `AND`, so assert on the predicate the replicas actually receive. The `NOT` query
-- must not send a `not(and(...))` - that is the conjunct-dropping `NOT (a < 500)` - while the fully
-- convertible predicate must still be sent, which keeps this assertion honest.
SELECT count() FROM t_push_ast_index WHERE NOT (a < 500 AND arrayExists(x -> x < 86, [b])) FORMAT Null;
SELECT count() FROM t_push_ast_index WHERE a >= 900 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'the weakened conjunct is not pushed', countIf(query LIKE '%not(and(%') FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time > now() - 600
    AND type = 'QueryFinish' AND is_initial_query = 0 AND query LIKE '%mergeTreeAnalyzeIndexesUUID%';
SELECT 'a convertible predicate is still pushed', countIf(query LIKE '%greaterOrEquals(a,%') > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time > now() - 600
    AND type = 'QueryFinish' AND is_initial_query = 0 AND query LIKE '%mergeTreeAnalyzeIndexesUUID%';

DROP TABLE t_push_ast_index;
