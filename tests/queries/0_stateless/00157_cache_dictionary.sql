-- Tags: stateful, no-tsan, no-msan, no-asan, no-parallel
-- no-parallel: Heavy

DROP TABLE IF EXISTS test.hits_1m;

CREATE TABLE test.hits_1m AS test.hits
ENGINE = MergeTree
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy = 'default',
-- set index_granularity correctly to avoid time out
index_granularity = 8192,
index_granularity_bytes = 10485760;

SET max_execution_time = 300;
INSERT INTO test.hits_1m SELECT * FROM test.hits LIMIT 1000000
SETTINGS min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_block_size = 8192, max_insert_threads = 1, max_threads = 1, max_parallel_replicas=1;

CREATE DATABASE IF NOT EXISTS db_dict;
DROP DICTIONARY IF EXISTS db_dict.cache_hits;

CREATE DICTIONARY db_dict.cache_hits
(WatchID UInt64, UserID UInt64, SearchPhrase String)
PRIMARY KEY WatchID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'hits_1m' PASSWORD '' DB 'test'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(CACHE(SIZE_IN_CELLS 1 QUERY_WAIT_TIMEOUT_MILLISECONDS 60000));

SELECT count() FROM (SELECT WatchID, arrayDistinct(groupArray(dictGetUInt64( 'db_dict.cache_hits', 'UserID', toUInt64(WatchID)))) as arr
FROM test.hits_1m PREWHERE WatchID % 5 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

SELECT count() FROM (SELECT WatchID, arrayDistinct(groupArray(dictGetUInt64( 'db_dict.cache_hits', 'UserID', toUInt64(WatchID)))) as arr
FROM test.hits_1m PREWHERE WatchID % 7 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

SELECT count() FROM (SELECT WatchID, arrayDistinct(groupArray(dictGetUInt64( 'db_dict.cache_hits', 'UserID', toUInt64(WatchID)))) as arr
FROM test.hits_1m PREWHERE WatchID % 13 == 0 GROUP BY  WatchID order by length(arr) desc) WHERE arr = [0];

DROP DICTIONARY IF EXISTS db_dict.cache_hits;
DROP DATABASE IF  EXISTS db_dict;
DROP TABLE IF EXISTS hits_1m;
