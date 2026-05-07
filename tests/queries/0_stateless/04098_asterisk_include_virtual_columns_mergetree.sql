-- Tags: no-ordinary-database, no-fasttest, no-encrypted-storage, no-async-insert

-- Test that asterisk_include_virtual_columns works with MergeTree.
-- Previously this caused: "Virtual column _distance must be filled by range reader".

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_virtuals_mt;

CREATE TABLE test_virtuals_mt (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = 'local_disk', index_granularity=1;

INSERT INTO test_virtuals_mt SELECT number, number FROM numbers(10);

-- { echoOn }
SELECT * FROM test_virtuals_mt ORDER BY a SETTINGS asterisk_include_virtual_columns=1;
-- { echoOff }

DROP TABLE test_virtuals_mt;
