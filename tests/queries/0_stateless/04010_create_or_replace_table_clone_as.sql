-- Tags: no-replicated-database
-- Reproducer for https://github.com/ClickHouse/ClickHouse/issues/93906
-- CREATE OR REPLACE TABLE ... CLONE AS triggered LOGICAL_ERROR
-- "Query pipeline requires output, but no output buffer provided"
-- when `alter_partition_verbose_result` was enabled.

SET alter_partition_verbose_result = 1;

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;

CREATE TABLE source (c1 DateTime, c2 UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO source VALUES ('2024-01-01', 1), ('2024-01-02', 2), ('2024-01-03', 3);

-- First call creates a new table (target does not exist).
CREATE OR REPLACE TABLE target CLONE AS source ENGINE = MergeTree ORDER BY tuple();
SELECT * FROM target ORDER BY c2;

-- Second call replaces an existing table.
CREATE OR REPLACE TABLE target CLONE AS source ENGINE = MergeTree ORDER BY tuple();
SELECT * FROM target ORDER BY c2;

DROP TABLE source;
DROP TABLE target;
