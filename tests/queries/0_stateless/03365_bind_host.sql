-- Tags: shard, no-fasttest

DROP TABLE IF EXISTS mem1;
DROP TABLE IF EXISTS dist_1;
DROP TABLE IF EXISTS dist_2;
DROP TABLE IF EXISTS dist_fail;

CREATE TABLE mem1 (key Int) Engine=Memory();

CREATE TABLE dist_1 Engine=Distributed(test_shard_bind_host, currentDatabase(), mem1);

CREATE TABLE dist_2 Engine=Distributed(test_shard_bind_host_secure, currentDatabase(), mem1);

CREATE TABLE dist_fail Engine=Distributed(test_shard_bind_host_fail, currentDatabase(), mem1); -- { serverError NO_REMOTE_SHARD_AVAILABLE }

DROP TABLE IF EXISTS mem1;
DROP TABLE IF EXISTS dist_1;
DROP TABLE IF EXISTS dist_2;
DROP TABLE IF EXISTS dist_fail;

