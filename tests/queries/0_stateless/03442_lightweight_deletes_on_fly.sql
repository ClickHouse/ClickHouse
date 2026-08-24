DROP TABLE IF EXISTS test_deletes;

CREATE TABLE test_deletes (a UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_deletes VALUES (1) (2) (3);

SYSTEM STOP MERGES test_deletes;
SET mutations_sync = 0;
SET lightweight_deletes_sync = 0;

ALTER TABLE test_deletes DELETE WHERE a = 1 SETTINGS mutations_sync = 0;
DELETE FROM test_deletes WHERE a = 2 SETTINGS lightweight_deletes_sync = 0;

SELECT a FROM test_deletes SETTINGS apply_mutations_on_fly = 1;

DROP TABLE test_deletes;

CREATE TABLE test_deletes (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_deletes SELECT number, 0 FROM numbers(10000);

DELETE FROM test_deletes WHERE a >= 100 AND a < 200 SETTINGS lightweight_deletes_sync = 1;

SYSTEM STOP MERGES test_deletes;

ALTER TABLE test_deletes UPDATE b = 1 WHERE a >= 150 AND a < 250 SETTINGS mutations_sync = 0;
DELETE FROM test_deletes WHERE b = 1 SETTINGS lightweight_deletes_sync = 0;

SELECT count() FROM test_deletes SETTINGS apply_mutations_on_fly = 1;

DROP TABLE test_deletes;
