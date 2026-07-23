-- Tags: shard, no-fasttest

-- Test max_skip_unavailable_shards_num and max_skip_unavailable_shards_ratio settings.
-- Use remote() with explicit addresses. Use FORMAT Null for error-expected queries
-- to avoid partial output from local shards before the error is thrown.

SET send_logs_level = 'fatal';

-- 2 shards: 1 available, 1 unavailable
-- Baseline: skip_unavailable_shards = 1, no limits, should succeed
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1', system, one) SETTINGS skip_unavailable_shards = 1;

-- max_skip_unavailable_shards_num = 1: skipped = 1, not greater, should succeed
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1', system, one) SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 1;

-- max_skip_unavailable_shards_num = 0 (no limit): should succeed
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1', system, one) SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 0;

-- max_skip_unavailable_shards_ratio = 0.5: ratio is 1/2 = 0.5, not greater, should succeed
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1', system, one) SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_ratio = 0.5;

-- max_skip_unavailable_shards_ratio = 0.4: ratio is 1/2 = 0.5 > 0.4, should throw
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1', system, one) FORMAT Null SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_ratio = 0.4; -- { serverError TOO_MANY_UNAVAILABLE_SHARDS }

-- 3 shards: 1 available, 2 unavailable
-- max_skip_unavailable_shards_num = 2: skipped = 2, not greater, should succeed
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1,127.0.0.1:2', system, one) SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 2;

-- max_skip_unavailable_shards_num = 1: skipped = 2 > 1, should throw
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1,127.0.0.1:2', system, one) FORMAT Null SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 1; -- { serverError TOO_MANY_UNAVAILABLE_SHARDS }

-- max_skip_unavailable_shards_ratio = 0.7: ratio is 2/3 ≈ 0.667, not greater, should succeed
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1,127.0.0.1:2', system, one) SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_ratio = 0.7;

-- max_skip_unavailable_shards_ratio = 0.5: ratio is 2/3 ≈ 0.667 > 0.5, should throw
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1,127.0.0.1:2', system, one) FORMAT Null SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_ratio = 0.5; -- { serverError TOO_MANY_UNAVAILABLE_SHARDS }

-- Settings should have no effect when skip_unavailable_shards = 0
SELECT * FROM remote('127.0.0.1:9000,127.0.0.1:1', system, one) FORMAT Null SETTINGS skip_unavailable_shards = 0, max_skip_unavailable_shards_num = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }
