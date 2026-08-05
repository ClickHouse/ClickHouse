-- Tags: no-parallel, no-flaky-check, shard

-- Tests that `skip_unavailable_shards` / `skip_unavailable_shards_mode` set at the `Distributed`
-- engine level apply to SELECT queries when the query-level settings were not explicitly changed.
-- The underlying view on shard_1 throws before returning any data.

DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE VIEW shard_0.v_sus AS SELECT 0 AS x;
CREATE VIEW shard_1.v_sus AS SELECT throwIf(1) AS x;

-- The engine-level `unavailable_or_exception_before_processing` mode ignores the exception from shard_1.
CREATE TABLE dist_sus_skip (x UInt8) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', v_sus)
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

SELECT x FROM dist_sus_skip SETTINGS prefer_localhost_replica = 0;

-- The engine-level `unavailable` mode does not ignore it.
CREATE TABLE dist_sus_strict (x UInt8) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', v_sus)
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable';

-- `FORMAT Null` so that the row that shard_0 streams before shard_1's exception aborts the query
-- does not leak into the output (the read from both shards runs in parallel).
SELECT x FROM dist_sus_strict SETTINGS prefer_localhost_replica = 0 FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- An explicitly changed query-level mode takes precedence over the engine-level one.
SELECT x FROM dist_sus_strict SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

DROP TABLE dist_sus_skip;
DROP TABLE dist_sus_strict;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
