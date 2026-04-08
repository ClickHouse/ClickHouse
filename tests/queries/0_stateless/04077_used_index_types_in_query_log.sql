-- Tags: no-parallel

SET log_queries = 1;

DROP TABLE IF EXISTS t_used_index_types;

CREATE TABLE t_used_index_types
(
    key UInt64,
    value String,
    INDEX idx_set value TYPE set(100) GRANULARITY 1,
    INDEX idx_bf value TYPE bloom_filter() GRANULARITY 1,
    INDEX idx_mm key TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO t_used_index_types SELECT number, toString(number) FROM numbers(1000);

-- Query uses primary key + set + bloom_filter + minmax skip indexes.
SELECT * FROM t_used_index_types WHERE key > 500 AND value = '42' SETTINGS use_skip_indexes = 1 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT arraySort(used_index_types)
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%t_used_index_types%key > 500%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY query_start_time DESC
LIMIT 1;

DROP TABLE t_used_index_types;
