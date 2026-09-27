-- When the query plan is shipped to a worker, the `IN` set of an `ENGINE = Set` table travels as a
-- table name and the worker rebuilds it (`makeSetsFromStorage`) before rerunning index analysis. That
-- is a second place where the set's mutability has to be declared, so pin the same rule there: the
-- text index must prune nothing for a set that lives in a table, while a set the query builds for
-- itself still prunes. Asserted through `read_rows`, since the worker's index analysis shows up
-- neither in the result nor in `EXPLAIN`.
--
-- Parallel replicas are pinned off rather than tagged away: under `automatic_parallel_replicas_mode`
-- the read is planned elsewhere and the set the query owns stops pruning too, which would leave the
-- comparison below with nothing to say. Pinning keeps the test running in the parallel-replicas run.

DROP TABLE IF EXISTS t_map_text_index;
DROP TABLE IF EXISTS values_set;

CREATE TABLE values_set (v String) ENGINE = Set;
INSERT INTO values_set VALUES ('val0'), ('val1'), ('val2');

CREATE TABLE t_map_text_index
(
    id UInt64,
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;

INSERT INTO t_map_text_index VALUES (0, {'hello world':'val0'}), (1, {'foo bar':'val1'}), (2, {'baz qux':'val2'});

SELECT id FROM remote('127.0.0.1', currentDatabase(), t_map_text_index)
WHERE m['hello world'] IN values_set AND 'text_index_mutable_set' != ''
ORDER BY id
SETTINGS serialize_query_plan = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT id FROM remote('127.0.0.1', currentDatabase(), t_map_text_index)
WHERE m['hello world'] IN (SELECT arrayJoin(['val0', 'val1', 'val2'])) AND 'text_index_owned_set' != ''
ORDER BY id
SETTINGS serialize_query_plan = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT '-- every granule is read for a set that lives in a table';
SELECT argMax(read_rows, event_time_microseconds) = 3
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
  AND event_time >= now() - INTERVAL 5 MINUTE
  AND query LIKE '%text\_index\_mutable\_set%' AND query NOT LIKE '%system.query\_log%';

SELECT '-- and the index still prunes for a set the query owns';
SELECT argMax(read_rows, event_time_microseconds) < 3
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
  AND event_time >= now() - INTERVAL 5 MINUTE
  AND query LIKE '%text\_index\_owned\_set%' AND query NOT LIKE '%system.query\_log%';

DROP TABLE t_map_text_index;
DROP TABLE values_set;
