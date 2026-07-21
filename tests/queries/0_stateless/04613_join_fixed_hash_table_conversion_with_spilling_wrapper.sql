-- A non-zero external-join threshold (the default) wraps the hash join in `SpillingHashJoin`.
-- The wrapper must forward the post-build phase to the promoted join to enable fixed hash table conversion.

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

CREATE TABLE t_build (k Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_build SELECT number FROM numbers(100);
INSERT INTO t_probe SELECT toInt32(number % 250) FROM numbers(20000);

SET join_algorithm = 'hash', enable_join_fixed_hash_table_conversion = 1;
SET max_bytes_before_external_join = '1G', max_bytes_ratio_before_external_join = 0;
SET enable_parallel_replicas = 0;

SELECT '-- join under spilling wrapper';
SELECT count() FROM t_probe p INNER JOIN t_build b ON p.k = b.k SETTINGS log_comment = '04614_convert_with_spilling';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT '-- wrapper used, converted';
SELECT countIf(logger_name = 'SpillingHashJoin' AND message LIKE '%promoting HashJoin%') > 0 AS used_spilling_wrapper,
       countIf(logger_name = 'HashJoin' AND message LIKE '%Converted join hash map to fixed hash map%') > 0 AS converted
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04614_convert_with_spilling' AND current_database = currentDatabase()
                AND type = 'QueryFinish' AND event_date >= yesterday());

DROP TABLE t_build;
DROP TABLE t_probe;
