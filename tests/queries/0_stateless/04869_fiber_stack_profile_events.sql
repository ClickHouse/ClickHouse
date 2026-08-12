-- Tags: shard

-- Fibers are used for asynchronous communication with remote replicas, and a stack is the only thing they consist of,
-- so it is worth observing how these stacks are allocated.

SELECT count() FROM remote('127.0.0.{1,2}', system.one)
SETTINGS async_socket_for_remote = 1, prefer_localhost_replica = 0, log_comment = '04869_fiber_stack_profile_events';

SYSTEM FLUSH LOGS query_log;

-- Every fiber allocates a stack of at least `FiberStack::default_stack_size` bytes, and the time of allocation is accounted for.
SELECT
    ProfileEvents['FiberStackAllocs'] > 0,
    ProfileEvents['FiberStackAllocBytes'] >= ProfileEvents['FiberStackAllocs'] * 320 * 1024,
    ProfileEvents['FiberStackAllocNanoseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment = '04869_fiber_stack_profile_events';

-- The stacks of the finished fibers are released, so the metrics cannot go negative.
SELECT count() FROM system.metrics WHERE metric IN ('FiberStacks', 'FiberStackBytes') AND value >= 0;
