-- The test is mostly outdated as now every replica is leader and can do OPTIMIZE locally.

DROP TABLE IF EXISTS rename1;
DROP TABLE IF EXISTS rename2;
DROP TABLE IF EXISTS rename3;
CREATE TABLE rename1 (p Int64, i Int64, v UInt64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/tables/rename', '1', v) PARTITION BY p ORDER BY i;
CREATE TABLE rename2 (p Int64, i Int64, v UInt64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/tables/rename', '2', v) PARTITION BY p ORDER BY i;

INSERT INTO rename1 VALUES (0, 1, 0);
INSERT INTO rename1 VALUES (0, 1, 1);

OPTIMIZE TABLE rename1;
OPTIMIZE TABLE rename2;
SELECT * FROM rename1;

RENAME TABLE rename2 TO rename3;

INSERT INTO rename1 VALUES (0, 1, 2);
SYSTEM SYNC REPLICA rename3; -- Make "rename3" to see all data parts.
OPTIMIZE TABLE rename3;
SYSTEM SYNC REPLICA rename1; -- Make "rename1" to see and process all scheduled merges.
SELECT * FROM rename1;

DROP TABLE IF EXISTS rename1;
DROP TABLE IF EXISTS rename2;
DROP TABLE IF EXISTS rename3;
