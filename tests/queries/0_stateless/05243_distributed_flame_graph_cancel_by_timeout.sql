-- The initiator times out while waiting for the shards: the constant below is cheap on the initiator
-- (shardNum() = 0) and much slower on the shards, which only then send their header of type
-- AggregateFunction(flameGraph, Array(UInt64)). Reading that header while cancelling must not crash the server.
-- This is timing-dependent and does not reproduce on every run, but repeated CI runs catch it.
SELECT flameGraph(trace)
FROM remote('127.0.0.{1,2}', view(
    SELECT [1::UInt64] AS trace
    WHERE arraySum(arrayMap(x -> sipHash64(x, x, x), range(if(shardNum() = 0, 300000, 10000000)))) != 1))
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw', allow_introspection_functions = 1, prefer_localhost_replica = 0; -- { serverError TIMEOUT_EXCEEDED }

SELECT 1;
