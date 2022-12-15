-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE IF EXISTS OLD;

-- insert entry into query result cache
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- save hash of (only) entry in query result cache
CREATE TABLE OLD (query_hash UInt64) ENGINE=MergeTree ORDER BY query_hash;
insert into OLD SELECT query_hash FROM system.queryresult_cache;

-- run same query but with different case
-- should still have just one entry with same hash as before
select 1 settings enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;
select query_hash = (select query_hash from OLD) from system.queryresult_cache;

DROP TABLE OLD;
SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
