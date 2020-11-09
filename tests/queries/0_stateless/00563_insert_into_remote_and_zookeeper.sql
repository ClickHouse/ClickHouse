-- Check that settings are correctly passed through Distributed table
DROP TABLE IF EXISTS simple;
CREATE TABLE simple (d Int8) ENGINE = ReplicatedMergeTree('/clickhouse/test/tables/test/simple', '1') ORDER BY d;

-- TODO: replace '127.0.0.2' -> '127.0.0.1' after a fix
INSERT INTO TABLE FUNCTION remote('127.0.0.2', currentDatabase(), 'simple') VALUES (1);
INSERT INTO TABLE FUNCTION remote('127.0.0.2', currentDatabase(), 'simple') VALUES (1);

SET insert_deduplicate=0;
INSERT INTO TABLE FUNCTION remote('127.0.0.2', currentDatabase(), 'simple') VALUES (2);
INSERT INTO TABLE FUNCTION remote('127.0.0.2', currentDatabase(), 'simple') VALUES (2);

SELECT * FROM remote('127.0.0.2', currentDatabase(), 'simple') ORDER BY d;
DROP TABLE simple;
