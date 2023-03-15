-- This is a regression test for EINTR handling in MultiplexedConnections::getReplicaForReading()

select * from remote('127.{2,4}', view(
    -- This is the emulation of the slow query, the server will return a line each 0.1 second
    select sleep(0.1) from numbers(20) settings max_block_size=1)
)
-- LIMIT is to activate query cancellation in case of enough rows already read.
limit 10
settings
    -- This is to avoid draining in background and got the exception during query execution
    drain_timeout=-1,
    -- This is to activate as much signals as possible to trigger EINTR
    query_profiler_real_time_period_ns=1,
    -- This is to use MultiplexedConnections
    use_hedged_requests=0,
    -- This is to make the initiator waiting for cancel packet in MultiplexedConnections::getReplicaForReading()
    --
    -- NOTE: that even smaller sleep will be enough to trigger this problem
    -- with 100% probability, however just to make it more reliable, increase
    -- it to 2 seconds.
    sleep_in_receive_cancel_ms=2000;
