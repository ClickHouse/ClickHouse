create table dist as system.one engine=Distributed(test_shard_localhost, system, one);
select sleep(3) from dist settings prefer_localhost_replica=0, receive_timeout=1, async_socket_for_remote=0, use_hedged_requests=1 format Null;
select sleep(3) from dist settings prefer_localhost_replica=0, receive_timeout=1, async_socket_for_remote=1, use_hedged_requests=0 format Null;
select sleep(3) from dist settings prefer_localhost_replica=0, receive_timeout=1, async_socket_for_remote=0, use_hedged_requests=0 format Null;
