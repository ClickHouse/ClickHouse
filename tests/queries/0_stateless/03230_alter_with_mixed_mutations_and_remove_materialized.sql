DROP TABLE IF EXISTS a_r1 SYNC;
DROP TABLE IF EXISTS a_r2 SYNC;
CREATE TABLE a_r1 (x String, y String MATERIALIZED 'str') ENGINE = ReplicatedMergeTree('/clickhouse/{database}/a', 'r1') ORDER BY x;
CREATE TABLE a_r2 (x String, y String MATERIALIZED 'str') ENGINE = ReplicatedMergeTree('/clickhouse/{database}/a', 'r2') ORDER BY x;

INSERT INTO a_r1 SELECT toString(number) FROM numbers(100);
SELECT 'BEFORE', table, name, type, default_kind, default_expression FROM system.columns WHERE database = currentDatabase() AND table LIKE 'a\_r%' ORDER BY table, name;

ALTER TABLE a_r1
	ADD INDEX IF NOT EXISTS some_index x TYPE set(16) GRANULARITY 1,
	MODIFY COLUMN y REMOVE MATERIALIZED
SETTINGS alter_sync = 2, mutations_sync = 2;

SELECT 'AFTER', table, name, type, default_kind, default_expression FROM system.columns WHERE database = currentDatabase() AND table LIKE 'a\_r%' ORDER BY table, name;
