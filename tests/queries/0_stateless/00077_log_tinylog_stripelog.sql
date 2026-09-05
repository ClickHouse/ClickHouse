-- Tags: stateful, long, no-flaky-check
-- no-flaky-check: full scans of test.hits per Log-family engine approach the 180 s per-test budget on sanitizer builds
SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS hits_log;
DROP TABLE IF EXISTS hits_tinylog;
DROP TABLE IF EXISTS hits_stripelog;

CREATE TABLE hits_log (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID UInt64) ENGINE = Log;
CREATE TABLE hits_tinylog (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID UInt64) ENGINE = TinyLog;
CREATE TABLE hits_stripelog (CounterID UInt32, AdvEngineID UInt8, RegionID UInt32, SearchPhrase String, UserID UInt64) ENGINE = StripeLog;

CHECK TABLE hits_log;
CHECK TABLE hits_tinylog;
CHECK TABLE hits_stripelog;

INSERT INTO hits_log SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID FROM test.hits;
INSERT INTO hits_tinylog SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID FROM test.hits;
INSERT INTO hits_stripelog SELECT CounterID, AdvEngineID, RegionID, SearchPhrase, UserID FROM test.hits;

SELECT count(), sum(cityHash64(CounterID, AdvEngineID, RegionID, SearchPhrase, UserID)) FROM test.hits;
SELECT count(), sum(cityHash64(*)) FROM hits_log;
SELECT count(), sum(cityHash64(*)) FROM hits_tinylog;
SELECT count(), sum(cityHash64(*)) FROM hits_stripelog;

CHECK TABLE hits_log;
CHECK TABLE hits_tinylog;
CHECK TABLE hits_stripelog;

DROP TABLE hits_log;
DROP TABLE hits_tinylog;
DROP TABLE hits_stripelog;
