-- Tags: no-parallel
-- Tag no-parallel: failpoint use_delayed_remote_source is global and can force
-- DelayedSource on concurrent tests and break them (same pattern as 02863, 04303).

-- Regression test: a distributed query that references an unbuilt MATERIALIZED CTE as an
-- external table crashed the server. ReadFromRemote::addLazyPipe built its RemoteQueryExecutor
-- without calling setLogger(), unlike every other construction site, so the executor's logger
-- stayed null. RemoteQueryExecutor::sendExternalTables() then logged "Skipping sending CTE ..."
-- on the unbuilt-CTE branch through that null logger, dereferencing it inside an async fiber
-- (Poco::Logger::is -> SIGSEGV). The fix sets the logger on the lazy path and the log site is
-- defensively guarded like its siblings in the same file.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- Force the lazy remote source path (the one that built RemoteQueryExecutor without a logger).
SYSTEM ENABLE FAILPOINT use_delayed_remote_source;

-- Two correlated IN subqueries over the materialized CTE produce external-table sends while the
-- CTE is still unbuilt, hitting the skip-CTE branch on the lazy (previously null-logger) path.
WITH t AS MATERIALIZED (SELECT number AS x FROM numbers(7))
SELECT * FROM remote('127.0.0.1', numbers(10))
WHERE ((SELECT * FROM t WHERE x < -2147483647) IN (number)) OR ((SELECT * FROM t WHERE x > 5) IN (number))
ORDER BY number DESC LIMIT 1023;

SYSTEM DISABLE FAILPOINT use_delayed_remote_source;
