-- Tags: no-parallel
-- Tag no-parallel: failpoint use_delayed_remote_source is global and can force
-- DelayedSource on concurrent tests and break them (same pattern as 02863).

-- Regression test: ReadFromRemote::addLazyPipe resolved the main table StorageID
-- unconditionally, but a table function such as remote(..., numbers(...)) has an empty main
-- table, so it threw "Both table name and UUID are empty" (UNKNOWN_TABLE) once the
-- use_delayed_remote_source failpoint forced the lazy remote read path. addPipe guards the
-- same resolution with a table-function check.

SET allow_experimental_parallel_reading_from_replicas = 0;

SYSTEM ENABLE FAILPOINT use_delayed_remote_source;

-- The lazy path is actually taken (DelayedSource in the pipeline) and building it no longer throws.
SELECT max(explain like '%Delayed%') FROM (EXPLAIN PIPELINE graph = 1 SELECT sum(number) FROM remote('127.0.0.1', numbers(10)));

SELECT sum(number) FROM remote('127.0.0.1', numbers(10));
SELECT count() FROM cluster('test_cluster_two_shards_localhost', numbers(10));
SELECT number FROM view(SELECT number FROM remote('127.0.0.1', numbers(3))) WHERE number = 1 ORDER BY number;

SYSTEM DISABLE FAILPOINT use_delayed_remote_source;
