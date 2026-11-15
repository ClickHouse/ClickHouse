-- Tags: no-parallel-replicas
-- (parallel replicas dispatch to all replicas at once, bypassing the `load_balancing` replica choice under test)

-- `least_request` is accepted as a `load_balancing` value and round-trips.
SET load_balancing = 'least_request';
SELECT value FROM system.settings WHERE name = 'load_balancing';
SELECT name, value FROM system.settings WHERE name LIKE 'load_balancing_least_request%' ORDER BY name;

-- A trivial distributed query works with this policy: two shards with one replica each,
SELECT count() FROM remote('127.0.0.{1,2}', system.one) SETTINGS prefer_localhost_replica = 0;
-- one shard with two replicas,
SELECT count() FROM remote('127.0.0.{1|2}', system.one) SETTINGS prefer_localhost_replica = 0;
-- and the degenerate full scan with a custom bias.
SELECT count() FROM remote('127.0.0.{1|2}', system.one) SETTINGS prefer_localhost_replica = 0, load_balancing_least_request_choice_count = 100, load_balancing_least_request_active_request_bias = 2.5;
