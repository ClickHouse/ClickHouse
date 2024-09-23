-- Tags: no-fasttest
-- no-fasttest: Timeouts are slow
create table dist as system.one engine=Distributed(test_shard_localhost, system, one);
select sleep(8) from dist settings function_sleep_max_microseconds_per_block=8e9, prefer_localhost_replica=0, receive_timeout=7, async_socket_for_remote=0, use_hedged_requests=1 format Null;
select sleep(8) from dist settings function_sleep_max_microseconds_per_block=8e9, prefer_localhost_replica=0, receive_timeout=7, async_socket_for_remote=1, use_hedged_requests=0 format Null;
select sleep(8) from dist settings function_sleep_max_microseconds_per_block=8e9, prefer_localhost_replica=0, receive_timeout=7, async_socket_for_remote=0, use_hedged_requests=0 format Null;
