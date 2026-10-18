-- Tags: shard

-- With parallel replicas over a custom key, `skip_unavailable_shards` counts every replica connection of the shard:
-- one skipped while another answered keeps the partial result, all of them skipped fails the query.

DROP TABLE IF EXISTS t_custom_key_skip_dead;
DROP TABLE IF EXISTS t_custom_key_skip;

CREATE TABLE t_custom_key_skip (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_custom_key_skip SELECT number FROM numbers(10);

CREATE TABLE t_custom_key_skip_dead (x UInt64) ENGINE = Remote('127.0.0.1:1|127.0.0.2:1', currentDatabase(), t_custom_key_skip);

SET prefer_localhost_replica = 0, serialize_query_plan = 0, automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'x';
SET skip_unavailable_shards = 1;

-- The replica reading odd keys fails before returning data, the one reading even keys answers.
SELECT sum(x + throwIf(x % 2 = 1)) FROM remote('127.0.0.{1|2}', currentDatabase(), t_custom_key_skip)
SETTINGS skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

SELECT count() FROM t_custom_key_skip_dead; -- { serverError ALL_CONNECTION_TRIES_FAILED }

DROP TABLE t_custom_key_skip_dead;
DROP TABLE t_custom_key_skip;
