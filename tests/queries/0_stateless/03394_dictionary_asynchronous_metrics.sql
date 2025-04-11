-- Tags: no-parallel
-- Tag no-parallel -- system.asynchronous_metrics could affected by other tests

DROP DICTIONARY IF EXISTS d1;
DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (key String, value String)
ENGINE = MergeTree()
PRIMARY KEY key;
CREATE DICTIONARY d1 (key String, value String) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE t1)) LAYOUT(FLAT()) LIFETIME(min 0 max 0);

SYSTEM RELOAD DICTIONARY d1;

SELECT name, value, description FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT name, value, description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';

DETACH TABLE t1;

SYSTEM RELOAD DICTIONARY d1; -- {serverError UNKNOWN_TABLE}
select sleep(1);
SYSTEM RELOAD ASYNCHRONOUS METRICS;

SELECT name, value, description FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT name, if(value >= 1, 'ok', 'fail: ' || toString(value)), description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';

SYSTEM RELOAD DICTIONARY d1; -- {serverError UNKNOWN_TABLE}
select sleep(1);
SYSTEM RELOAD ASYNCHRONOUS METRICS;

SELECT error_count FROM system.dictionaries WHERE name = 'd1';

SELECT name, value, description FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT name, if(value >= 2, 'ok', 'fail: ' || toString(value)) description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';

ATTACH TABLE t1;
DROP DICTIONARY IF EXISTS d1;
DROP TABLE IF EXISTS t1;

-- Check metrics after dropping table
SYSTEM RELOAD ASYNCHRONOUS METRICS;

SELECT name, value, description FROM system.asynchronous_metrics WHERE name = 'DictionaryTotalFailedUpdates';
SELECT name, value, description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxUpdateDelay';
