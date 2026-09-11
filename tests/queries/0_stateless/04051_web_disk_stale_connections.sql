-- Tags: stateful, long, no-parallel
-- Tag no-parallel: this test needs `test.hits` reads to go over the (idle, staleness-prone) HTTP
-- connection pool rather than be served warm, so it issues global, unscoped
-- `SYSTEM DROP MARK CACHE`/`SYSTEM DROP FILESYSTEM CACHE` (no named filesystem cache to scope to
-- is known for `test.hits`'s disk in every storage flavor this test runs under). It spans both the
-- `mark-cache` and `filesystem-cache` groups at once, so it cannot be placed in either single
-- group without risking those groups' own assertions.

-- Reproducer for stale HTTP connection pool issue with web disk.
-- S3 silently closes idle keep-alive connections after ~5-20s, but the pool's
-- default keep-alive timeout is 30s. When the pool returns a stale connection,
-- Poco throws NoMessageException ("No message received").
-- The fix detects stale connections via poll(SELECT_READ, 0) in getConnection.

SYSTEM DROP MARK CACHE;
SYSTEM DROP FILESYSTEM CACHE;

SELECT ignore(WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,ClientIP6,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,URLDomain,RefererDomain,Refresh,IsRobot) FROM test.hits LIMIT 1 SETTINGS max_threads = 64 FORMAT Null;

SELECT sleep(1) FROM numbers(6) SETTINGS max_block_size = 1 FORMAT Null;

SYSTEM DROP MARK CACHE;
SYSTEM DROP FILESYSTEM CACHE;

-- (We use nondefault http_retry_max_backoff_ms here, but the problem was possible with default
--  settings too, if concurrent queries release connections at just the right times. But that was difficult to reproduce.)
SELECT ignore(WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,ClientIP6,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,URLDomain,RefererDomain,Refresh,IsRobot) FROM test.hits LIMIT 1 SETTINGS max_threads = 64, http_retry_max_backoff_ms = 1000;
