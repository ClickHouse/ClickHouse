-- A block nested loop join that writes its build side to temporary files is listed in `spilled_to_disk`.

SET log_queries = 1;
SET cross_to_inner_join_rewrite = 0, join_algorithm = 'partial_merge', allow_block_nested_loop_join = 1;

SELECT count() FROM (
    EXPLAIN SELECT count() FROM (SELECT number % 9 AS x FROM numbers(40)) l LEFT JOIN (SELECT number % 6 AS y FROM numbers(30)) r ON l.x < r.y)
WHERE explain LIKE '%BlockNestedLoopJoin%';

SELECT count() FROM (SELECT number % 9 AS x FROM numbers(40)) l LEFT JOIN (SELECT number % 6 AS y FROM numbers(30)) r ON l.x < r.y
FORMAT Null
SETTINGS log_comment = '05339_bnl_spilled', max_bytes_before_external_join = 1, max_block_size = 6;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, spilled_to_disk, ProfileEvents['ExternalJoinWritePart'] > 0 AS join_wrote_temporary_files
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment = '05339_bnl_spilled';
