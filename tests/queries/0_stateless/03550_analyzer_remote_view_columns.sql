CREATE TABLE test
(
    `i1` Int64,
    `i2` Int64,
    `i3` Int64,
    `i4` Int64,
    `i5` Int64,
    `i6` Int64,
    `i7` Int64,
    `i8` Int64,
    `i9` Int64,
    `i10` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;

CREATE VIEW test_view
AS SELECT *
FROM test;

SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;

SELECT max(i1)
FROM remote('localhost', currentDatabase(), test_view)
SETTINGS log_comment = 'THIS IS A COMMENT TO MARK THE INITIAL QUERY';

SYSTEM FLUSH LOGS query_log;

SELECT columns
FROM system.query_log
WHERE
    initial_query_id = (
        SELECT query_id
        FROM system.query_log
        WHERE
            current_database = currentDatabase()
            AND log_comment = 'THIS IS A COMMENT TO MARK THE INITIAL QUERY'
        LIMIT 1)
    AND type = 'QueryFinish'
    AND NOT is_initial_query;
