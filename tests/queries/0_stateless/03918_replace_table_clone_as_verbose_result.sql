-- Tags: no-replicated-database
-- Reproducer for LOGICAL_ERROR in REPLACE TABLE ... CLONE AS
-- when alter_partition_verbose_result = true.

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;

CREATE TABLE source (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE target (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO source VALUES (1), (2), (3);

SET alter_partition_verbose_result = 1;

REPLACE TABLE target CLONE AS source ENGINE = MergeTree ORDER BY x;

SELECT * FROM target ORDER BY x;

DROP TABLE source;
DROP TABLE target;
