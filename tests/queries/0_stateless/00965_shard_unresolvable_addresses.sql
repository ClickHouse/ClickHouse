-- Tags: shard

SET prefer_localhost_replica = 1;

SELECT count() FROM remote('127.0.0.1,localhos', system.one); -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT count() FROM remote('127.0.0.1|localhos', system.one);

-- Clear cache to avoid future errors in the logs
SYSTEM DROP DNS CACHE
