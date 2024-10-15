DROP TABLE IF EXISTS tt;
CREATE TABLE tt (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO tt SELECT * FROM numbers(10);

SET enable_parallel_replicas=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1;
SELECT count() FROM remote('127.0.0.{1..6}', currentDatabase(), tt) settings log_comment='02875_89f3c39b-1919-48cb-b66e-ef9904e73146';

SYSTEM FLUSH LOGS;

SET enable_parallel_replicas=0;
SET max_rows_to_read = 0; -- system.text_log can be really big
SELECT count() > 0 FROM system.text_log
WHERE query_id in (select query_id from system.query_log where current_database = currentDatabase() AND log_comment = '02875_89f3c39b-1919-48cb-b66e-ef9904e73146')
    AND message LIKE '%Parallel reading from replicas is disabled for cluster%';

DROP TABLE tt;
