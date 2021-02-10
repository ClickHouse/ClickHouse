-- Somehow this one is too fast in release mode. Moreover, the very parsing
-- can be slow, because the constructor of Cluster::Address makes DNS queries
-- to determine whether an address is local. I have to switch this to deferred
-- initialization, but this is too much work for now.
-- select * from remote('badhost{0..200}.test', system, one) settings max_execution_time = 1; -- { serverError 159 }

-- addresses from TEST-NET-1,2
select * from remote('{192.0.2,198.51.100.0}.{0..255}', default, nonexistent01566)
settings max_execution_time = 1, table_function_remote_max_addresses = 100000; -- { serverError 159 }
