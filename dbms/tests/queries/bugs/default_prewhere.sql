DROP TABLE IF EXISTS test_generic_events_all;
CREATE TABLE test_generic_events_all (APIKey UInt8, SessionType UInt8) ENGINE = MergeTree() PARTITION BY APIKey ORDER BY tuple();
INSERT INTO test_generic_events_all VALUES( 42, 42 );
ALTER TABLE test_generic_events_all ADD COLUMN OperatingSystem UInt64 DEFAULT APIKey+1;
SELECT * FROM test_generic_events_all WHERE APIKey = 42 AND SessionType = 42;
-- InterpreterSelectQuery: MergeTreeWhereOptimizer: condition "APIKey = 42" moved to PREWHERE
SELECT OperatingSystem FROM test_generic_events_all WHERE APIKey = 42;
SELECT OperatingSystem FROM test_generic_events_all WHERE APIKey = 42 AND SessionType = 42;
SELECT OperatingSystem FROM test_generic_events_all PREWHERE APIKey = 42 WHERE SessionType = 42;
SELECT * FROM test_generic_events_all PREWHERE APIKey = 42 WHERE SessionType = 42;

DROP TABLE IF EXISTS test_generic_events_all;
