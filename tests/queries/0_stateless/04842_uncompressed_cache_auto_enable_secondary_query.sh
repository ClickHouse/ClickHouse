#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-parallel-replicas
# Tag no-random-settings: the test asserts uncompressed cache profile events, which a randomized
# `use_uncompressed_cache` would distort.
# Tag no-object-storage: automatic mode never applies to parts on object storage.
# Tag no-parallel-replicas: the test inspects the profile events of the secondary queries it issues itself.
#
# An explicit `use_uncompressed_cache = 0` must win over `enable_automatic_use_uncompressed_cache = 1`
# on the shards as well, not only on the initiator. The opt-out is carried solely by the `changed` flag
# of a setting whose value equals the default, and the leaf server drops such a change while clamping the
# forwarded settings to its own constraints - so the initiator has to switch the automatic mode off in
# the settings it sends out.
#
# The secondary queries run under the `default` database (only the tables they read are qualified), so
# they are told apart by `databases` plus a log comment that carries the test database name - otherwise
# concurrent runs of this test would read each other's rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AUTO_RUN_1="04842_auto_run_1_${CLICKHOUSE_DATABASE}"
AUTO_RUN_2="04842_auto_run_2_${CLICKHOUSE_DATABASE}"
OPT_OUT_RUN="04842_opt_out_run_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_uncompressed_cache_secondary;

CREATE TABLE t_uncompressed_cache_secondary
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_uncompressed_cache_secondary SELECT number, repeat('x', 128) FROM numbers(32768);

SET log_queries = 1;
SET enable_automatic_use_uncompressed_cache = 1;

-- Control: without an opt-out the shard auto-enables the cache, and the second run finds it warm.
SELECT sum(length(payload)) = 32768 * 128
FROM remote('127.0.0.2', currentDatabase(), t_uncompressed_cache_secondary)
SETTINGS max_threads = 1, log_comment = '${AUTO_RUN_1}';

SELECT sum(length(payload)) = 32768 * 128
FROM remote('127.0.0.2', currentDatabase(), t_uncompressed_cache_secondary)
SETTINGS max_threads = 1, log_comment = '${AUTO_RUN_2}';

-- The session-level opt-out must reach the shard, even though the cache is already warm there.
SET use_uncompressed_cache = 0;

SELECT sum(length(payload)) = 32768 * 128
FROM remote('127.0.0.2', currentDatabase(), t_uncompressed_cache_secondary)
SETTINGS max_threads = 1, log_comment = '${OPT_OUT_RUN}';

SYSTEM FLUSH LOGS query_log;

-- The warm control run hits the cache on the shard.
SELECT ProfileEvents['UncompressedCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${AUTO_RUN_2}'
  AND query LIKE '%sum(length(%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- The opt-out run does not touch the uncompressed cache on the shard at all.
SELECT ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${OPT_OUT_RUN}'
  AND query LIKE '%sum(length(%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_uncompressed_cache_secondary;
"

# Parallel replicas forward the settings on their own path, so check it separately.
PR_AUTO_RUN="04842_pr_auto_run_${CLICKHOUSE_DATABASE}"
PR_OPT_OUT_RUN="04842_pr_opt_out_run_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_uncompressed_cache_parallel_replicas;

CREATE TABLE t_uncompressed_cache_parallel_replicas
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_uncompressed_cache_parallel_replicas SELECT number, repeat('x', 128) FROM numbers(32768);

SET log_queries = 1;
SET enable_automatic_use_uncompressed_cache = 1;
SET enable_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0;

-- Control: the replicas auto-enable the cache, so they read through it.
SELECT sum(length(payload)) = 32768 * 128 FROM t_uncompressed_cache_parallel_replicas
SETTINGS log_comment = '${PR_AUTO_RUN}';

SET use_uncompressed_cache = 0;

SELECT sum(length(payload)) = 32768 * 128 FROM t_uncompressed_cache_parallel_replicas
SETTINGS log_comment = '${PR_OPT_OUT_RUN}';

SYSTEM FLUSH LOGS query_log;

SELECT sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses']) > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${PR_AUTO_RUN}';

-- The row count guards against the aggregate silently summing over no rows at all.
SELECT count() > 0, sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'])
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${PR_OPT_OUT_RUN}';

DROP TABLE t_uncompressed_cache_parallel_replicas;
"
