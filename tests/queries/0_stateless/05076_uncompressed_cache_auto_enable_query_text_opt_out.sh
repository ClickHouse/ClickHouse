#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-parallel-replicas
# Tag no-random-settings: the test asserts uncompressed cache profile events, which a randomized
# `use_uncompressed_cache` would distort.
# Tag no-object-storage: automatic mode never applies to parts on object storage.
# Tag no-parallel-replicas: the test inspects the profile events of the secondary queries it issues itself.
#
# The initiator bakes an explicit `use_uncompressed_cache = 0` into the settings packet of a secondary query,
# but the remote server also replays the query-level `SETTINGS` clause of the forwarded query text after it
# has clamped that default-valued change away. So a `SETTINGS enable_automatic_use_uncompressed_cache = 1`
# (or a `SETTINGS profile = '...'` that enables it) written in the query text would switch the automatic mode
# back on there, overriding the session-level opt-out. The initiator therefore has to bake the opt-out into
# the forwarded query text as well, on every carrier: the `remote` / `Distributed` fan-out, parallel
# replicas, and the parallel distributed `INSERT ... SELECT`.
#
# The secondary queries run under the `default` database (only the tables they read are qualified), so
# they are told apart by `databases` plus a log comment that carries the test database name - otherwise
# concurrent runs of this test would read each other's rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AUTO_RUN_1="05076_auto_run_1_${CLICKHOUSE_DATABASE}"
AUTO_RUN_2="05076_auto_run_2_${CLICKHOUSE_DATABASE}"
OPT_OUT_RUN="05076_opt_out_run_${CLICKHOUSE_DATABASE}"
PROFILE_OPT_OUT_RUN="05076_profile_opt_out_run_${CLICKHOUSE_DATABASE}"
AUTO_PROFILE="profile_05076_auto_${CLICKHOUSE_DATABASE}"
HTTP_SESSION="05076_session_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_uncompressed_cache_query_text;

CREATE TABLE t_uncompressed_cache_query_text
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_uncompressed_cache_query_text SELECT number, repeat('x', 128) FROM numbers(32768);

SET log_queries = 1;

-- Control: the automatic mode is enabled from the query text only, and the second run finds the shard cache warm.
SELECT sum(length(payload)) = 32768 * 128
FROM remote('127.0.0.2', currentDatabase(), t_uncompressed_cache_query_text)
SETTINGS enable_automatic_use_uncompressed_cache = 1, max_threads = 1, log_comment = '${AUTO_RUN_1}';

SELECT sum(length(payload)) = 32768 * 128
FROM remote('127.0.0.2', currentDatabase(), t_uncompressed_cache_query_text)
SETTINGS enable_automatic_use_uncompressed_cache = 1, max_threads = 1, log_comment = '${AUTO_RUN_2}';

-- The session-level opt-out must win on the shard over the automatic mode replayed from the query text.
SET use_uncompressed_cache = 0;

SELECT sum(length(payload)) = 32768 * 128
FROM remote('127.0.0.2', currentDatabase(), t_uncompressed_cache_query_text)
SETTINGS enable_automatic_use_uncompressed_cache = 1, max_threads = 1, log_comment = '${OPT_OUT_RUN}';

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
ORDER BY event_time_microseconds DESC
LIMIT 1;
"

# The same when the query text pulls the automatic mode in through a settings profile. This goes over HTTP
# because clickhouse-client applies a query-level SETTINGS clause on its own side first, where no settings
# profile exists; the session keeps the opt-out for the query that follows.
$CLICKHOUSE_CLIENT --query "
DROP SETTINGS PROFILE IF EXISTS ${AUTO_PROFILE};
CREATE SETTINGS PROFILE ${AUTO_PROFILE} SETTINGS enable_automatic_use_uncompressed_cache = 1;
"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${HTTP_SESSION}" -d "SET use_uncompressed_cache = 0"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${HTTP_SESSION}" -d "
SELECT sum(length(payload)) = 32768 * 128
FROM remote('127.0.0.2', currentDatabase(), t_uncompressed_cache_query_text)
SETTINGS profile = '${AUTO_PROFILE}', max_threads = 1, log_comment = '${PROFILE_OPT_OUT_RUN}'"

$CLICKHOUSE_CLIENT --query "
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${PROFILE_OPT_OUT_RUN}'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_uncompressed_cache_query_text;
DROP SETTINGS PROFILE ${AUTO_PROFILE};
"

# Parallel replicas forward the query text on their own path, so check it separately.
PR_AUTO_RUN="05076_pr_auto_run_${CLICKHOUSE_DATABASE}"
PR_OPT_OUT_RUN="05076_pr_opt_out_run_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_uncompressed_cache_query_text_pr;

CREATE TABLE t_uncompressed_cache_query_text_pr
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_uncompressed_cache_query_text_pr SELECT number, repeat('x', 128) FROM numbers(32768);

SET log_queries = 1;
SET enable_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0;

-- Control: the automatic mode enabled from the query text reaches the replicas, which read through the cache.
SELECT sum(length(payload)) = 32768 * 128 FROM t_uncompressed_cache_query_text_pr
SETTINGS enable_automatic_use_uncompressed_cache = 1, log_comment = '${PR_AUTO_RUN}';

SET use_uncompressed_cache = 0;

SELECT sum(length(payload)) = 32768 * 128 FROM t_uncompressed_cache_query_text_pr
SETTINGS enable_automatic_use_uncompressed_cache = 1, log_comment = '${PR_OPT_OUT_RUN}';

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

DROP TABLE t_uncompressed_cache_query_text_pr;
"

# The parallel distributed INSERT ... SELECT forwards a formatted query string of its own, so check it separately.
INSERT_AUTO_RUN="05076_insert_auto_run_${CLICKHOUSE_DATABASE}"
INSERT_OPT_OUT_RUN="05076_insert_opt_out_run_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_uncompressed_cache_query_text_src;
DROP TABLE IF EXISTS t_uncompressed_cache_query_text_dst;
DROP TABLE IF EXISTS dist_uncompressed_cache_query_text_src;
DROP TABLE IF EXISTS dist_uncompressed_cache_query_text_dst;

CREATE TABLE t_uncompressed_cache_query_text_src
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE t_uncompressed_cache_query_text_dst
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE dist_uncompressed_cache_query_text_src AS t_uncompressed_cache_query_text_src
ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_uncompressed_cache_query_text_src);

CREATE TABLE dist_uncompressed_cache_query_text_dst AS t_uncompressed_cache_query_text_dst
ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_uncompressed_cache_query_text_dst);

INSERT INTO t_uncompressed_cache_query_text_src SELECT number, repeat('x', 128) FROM numbers(32768);

SET log_queries = 1;
SET parallel_distributed_insert_select = 2;
SET prefer_localhost_replica = 0;

-- Control: the automatic mode enabled from the query text reaches the shard, which reads through the cache.
INSERT INTO dist_uncompressed_cache_query_text_dst
SELECT * FROM dist_uncompressed_cache_query_text_src
SETTINGS enable_automatic_use_uncompressed_cache = 1, log_comment = '${INSERT_AUTO_RUN}';

SET use_uncompressed_cache = 0;

INSERT INTO dist_uncompressed_cache_query_text_dst
SELECT * FROM dist_uncompressed_cache_query_text_src
SETTINGS enable_automatic_use_uncompressed_cache = 1, log_comment = '${INSERT_OPT_OUT_RUN}';

SELECT count() = 2 * 32768 FROM t_uncompressed_cache_query_text_dst;

SYSTEM FLUSH LOGS query_log;

SELECT count() > 0, sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses']) > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${INSERT_AUTO_RUN}';

-- The row count guards against the aggregate silently summing over no rows at all.
SELECT count() > 0, sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'])
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${INSERT_OPT_OUT_RUN}';

DROP TABLE dist_uncompressed_cache_query_text_src;
DROP TABLE dist_uncompressed_cache_query_text_dst;
DROP TABLE t_uncompressed_cache_query_text_src;
DROP TABLE t_uncompressed_cache_query_text_dst;
"
