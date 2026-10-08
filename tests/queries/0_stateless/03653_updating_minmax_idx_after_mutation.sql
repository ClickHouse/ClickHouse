DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    time DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(time) ORDER BY ();

INSERT INTO test VALUES ('2000-01-01 01:02:03'), ('2000-01-01 04:05:06');
SELECT max_time FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active;
SET mutations_sync = 1;
ALTER TABLE test DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT max_time FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active;

DROP TABLE test;
