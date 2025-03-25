DROP DICTIONARY IF EXISTS d1;
DROP DICTIONARY IF EXISTS d0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (key String, value String)
ENGINE = MergeTree()
PRIMARY KEY key;

INSERT INTO t0 VALUES ('key1', 'value1');

CREATE DICTIONARY d0 (key String, value String)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE t0))
LAYOUT(HASHED())
LIFETIME(min 0 max 0);

SELECT * FROM d0;

SELECT sleep(0.5);
SYSTEM RELOAD ASYNCHRONOUS METRICS;

SELECT * FROM system.asynchronous_metrics WHERE name = 'DictionaryLoadFailed';
SELECT name, value > 0.5, description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxLastSuccessfulUpdateTime';

-- Check failed dictionary d1
CREATE DICTIONARY d1 (key String, value String) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE t1)) LAYOUT(HASHED()) LIFETIME(min 0 max 0);

SELECT * FROM d1; -- {serverError 60}
select sleep(0.5);
SYSTEM RELOAD ASYNCHRONOUS METRICS;

SELECT * FROM system.asynchronous_metrics WHERE name = 'DictionaryLoadFailed';
SELECT name, value > 1.0, description FROM system.asynchronous_metrics WHERE name = 'DictionaryMaxLastSuccessfulUpdateTime';

DROP DICTIONARY IF EXISTS d1;
DROP DICTIONARY IF EXISTS d0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
