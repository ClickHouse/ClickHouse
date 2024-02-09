-- Tags: no-parallel

SET async_insert = 1;
SET insert_deduplicate = 1;
SET deduplicate_blocks_in_dependent_materialized_views = 1;

DROP TABLE IF EXISTS 02985_test;
CREATE TABLE 02985_test
(
    d Date,
    value UInt64
) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 1000;

DROP VIEW IF EXISTS 02985_mv;
CREATE MATERIALIZED VIEW 02985_mv
ENGINE = SummingMergeTree ORDER BY d AS
SELECT
    d, sum(value) s
FROM 02985_test GROUP BY d;

-- Inserts are synchronous.
INSERT INTO 02985_test (*)
VALUES ('2024-01-01', 1), ('2024-01-01', 2), ('2024-01-02', 1);

SYSTEM FLUSH LOGS;

SELECT format, status, rows, data_kind  FROM system.asynchronous_insert_log
WHERE database = currentDatabase() AND table = '02985_test';

SET deduplicate_blocks_in_dependent_materialized_views = 0;

-- Set a large value for async_insert_busy_timeout_max_ms to avoid flushing the entry synchronously.
INSERT INTO 02985_test (*)
SETTINGS
    async_insert_busy_timeout_min_ms=200,
    async_insert_busy_timeout_max_ms=100000
VALUES ('2024-01-01', 1), ('2024-01-01', 2), ('2024-01-02', 1), ('2024-01-02', 4);

SYSTEM FLUSH LOGS;

SELECT format, status, rows, data_kind
FROM system.asynchronous_insert_log
WHERE database = currentDatabase() AND table = '02985_test';

DROP VIEW IF EXISTS 02985_mv;
DROP TABLE IF EXISTS 02985_test;
