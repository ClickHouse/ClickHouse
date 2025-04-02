-- Tags: no-parallel
-- Tag no-parallel -- due to sleep calls

DROP DICTIONARY IF EXISTS d1;
DROP DICTIONARY IF EXISTS d0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;

CREATE TABLE t1 (key String, value String)
ENGINE = MergeTree()
PRIMARY KEY key;
CREATE DICTIONARY d1 (key String, value String) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE t1)) LAYOUT(FLAT()) LIFETIME(min 0 max 0);

SYSTEM RELOAD DICTIONARY d1;

SELECT * FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT name, value, description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';

DETACH TABLE t1;

SYSTEM RELOAD DICTIONARY d1; -- {serverError UNKNOWN_TABLE}
select sleep(1);
SYSTEM RELOAD ASYNCHRONOUS METRICS;

SELECT * FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT name, value >= 1, description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';

SYSTEM RELOAD DICTIONARY d1; -- {serverError UNKNOWN_TABLE}
select sleep(1);
SYSTEM RELOAD ASYNCHRONOUS METRICS;

SELECT error_count FROM system.dictionaries WHERE name = 'd1';

SELECT * FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT name, value >= 2, description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';

ATTACH TABLE t1;
DROP DICTIONARY IF EXISTS d1;
DROP DICTIONARY IF EXISTS d0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;

-- Check metrics after dropping table
SYSTEM RELOAD ASYNCHRONOUS METRICS;

SELECT * FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT * FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';
