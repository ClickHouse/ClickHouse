-- Tags: shard, no-parallel

-- Fibers are used for asynchronous communication with remote replicas, and a stack is the only thing they consist of,
-- so it is worth observing how these stacks are allocated and released.

CREATE TEMPORARY TABLE fiber_stack_metrics_before
ENGINE = Memory AS
SELECT metric, value
FROM system.metrics
WHERE metric IN ('FiberStacks', 'FiberStackBytes');

SELECT count() FROM remote('127.0.0.{1,2}', system.one)
SETTINGS async_socket_for_remote = 1, prefer_localhost_replica = 0, log_comment = '04869_fiber_stack_profile_events';

SYSTEM FLUSH LOGS query_log;

-- Every fiber allocates a stack of at least `FiberStack::default_stack_size` bytes, and the time of allocation is accounted for.
SELECT
    ProfileEvents['FiberStackAllocs'] > 0,
    ProfileEvents['FiberStackAllocBytes'] >= ProfileEvents['FiberStackAllocs'] * 320 * 1024,
    ProfileEvents['FiberStackAllocNanoseconds'] > 0,
    ProfileEvents['FiberStackFreeNanoseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment = '04869_fiber_stack_profile_events';

-- The stacks of the finished fibers are released, so both metrics return to their pre-query values.
SELECT count()
FROM system.metrics AS after
INNER JOIN fiber_stack_metrics_before AS before USING (metric)
WHERE after.value = before.value;
