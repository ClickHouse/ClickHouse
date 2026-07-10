-- Tags: shard, no-fasttest

-- Test load_balancing = 'first': the query always goes to the first replica
-- (the one at load_balancing_first_offset) and fails if it is unavailable,
-- without falling back to other replicas. Port 1 is used as an unavailable replica.

SET send_logs_level = 'fatal';

-- The first replica is alive: the query is served by it.
SELECT * FROM remote('127.0.0.1:9000|127.0.0.1:1', system, one) SETTINGS load_balancing = 'first', use_hedged_requests = 0;
SELECT * FROM remote('127.0.0.1:9000|127.0.0.1:1', system, one) SETTINGS load_balancing = 'first', use_hedged_requests = 1;

-- The first replica is unavailable: the query fails instead of falling back.
SELECT * FROM remote('127.0.0.1:1|127.0.0.1:9000', system, one) FORMAT Null SETTINGS load_balancing = 'first', use_hedged_requests = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT * FROM remote('127.0.0.1:1|127.0.0.1:9000', system, one) FORMAT Null SETTINGS load_balancing = 'first', use_hedged_requests = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- Unlike first_or_random, which falls back to the other replica.
SELECT * FROM remote('127.0.0.1:1|127.0.0.1:9000', system, one) SETTINGS load_balancing = 'first_or_random', use_hedged_requests = 0;

-- load_balancing_first_offset designates which replica is the first one.
SELECT * FROM remote('127.0.0.1:1|127.0.0.1:9000', system, one) SETTINGS load_balancing = 'first', load_balancing_first_offset = 1, use_hedged_requests = 0;
SELECT * FROM remote('127.0.0.1:1|127.0.0.1:9000', system, one) SETTINGS load_balancing = 'first', load_balancing_first_offset = 1, use_hedged_requests = 1;
SELECT * FROM remote('127.0.0.1:9000|127.0.0.1:1', system, one) FORMAT Null SETTINGS load_balancing = 'first', load_balancing_first_offset = 1, use_hedged_requests = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT * FROM remote('127.0.0.1:9000|127.0.0.1:1', system, one) FORMAT Null SETTINGS load_balancing = 'first', load_balancing_first_offset = 1, use_hedged_requests = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }
