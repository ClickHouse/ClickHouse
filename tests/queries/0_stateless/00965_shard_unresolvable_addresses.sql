-- Tags: shard, no-fasttest
-- no-fasttest: Slow timeouts

SET prefer_localhost_replica = 1;
SET connections_with_failover_max_tries=1;
SET connect_timeout_with_failover_ms=2000;
SET connect_timeout_with_failover_secure_ms=2000;

SELECT count() FROM remote('127.0.0.1,localhos', system.one); -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT count() FROM remote('127.0.0.1|localhos', system.one);
