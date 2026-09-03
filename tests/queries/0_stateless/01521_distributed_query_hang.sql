-- Tags: distributed

-- regression for endless loop with connections_with_failover_max_tries=0
set connections_with_failover_max_tries=0;
select * from remote('127.2', system.one);
