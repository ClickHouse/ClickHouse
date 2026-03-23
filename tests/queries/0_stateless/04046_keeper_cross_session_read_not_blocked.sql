-- Tags: no-fasttest, zookeeper

-- Verify that the KeeperReadImmediateProcessed and KeeperReadWaitForSameSessionWrite
-- profile events exist and are functional. These events track whether Keeper reads
-- bypass cross-session write commits (the fix for cross-session read blocking
-- in KeeperDispatcher).

SELECT event
FROM system.events
WHERE event IN ('KeeperReadImmediateProcessed', 'KeeperReadWaitForSameSessionWrite')
ORDER BY event;
