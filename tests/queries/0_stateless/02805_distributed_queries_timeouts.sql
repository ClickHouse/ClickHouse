select * from remote('127.2', view(select sleep(3) from system.one)) settings receive_timeout=1, async_socket_for_remote=0, use_hedged_requests=1 format Null;
select * from remote('127.2', view(select sleep(3) from system.one)) settings receive_timeout=1, async_socket_for_remote=1, use_hedged_requests=0 format Null;
select * from remote('127.2', view(select sleep(3) from system.one)) settings receive_timeout=1, async_socket_for_remote=0, use_hedged_requests=0 format Null;
