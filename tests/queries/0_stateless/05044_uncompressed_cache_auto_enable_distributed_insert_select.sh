#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-parallel-replicas
# Tag no-random-settings: the test asserts uncompressed cache profile events, which a randomized
# `use_uncompressed_cache` would distort.
# Tag no-object-storage: automatic mode never applies to parts on object storage.
# Tag no-parallel-replicas: the test inspects the profile events of the secondary queries it issues itself.
#
# The parallel distributed `INSERT ... SELECT` path builds its own per-shard context in
# `StorageDistributed::distributedWriteBetweenDistributedTables` and forwards it through
# `RemoteQueryExecutor`, so it is a separate carrier of the settings of a secondary query. An explicit
# `use_uncompressed_cache = 0` is carried solely by the `changed` flag of a setting whose value equals the
# default, and the shard drops such a change while clamping the forwarded settings to its own constraints -
# so this carrier has to switch the automatic mode off in the settings it sends out, just like the read
# fan-out does.
#
# `prefer_localhost_replica = 0` forces the shard to be reached over a connection instead of being executed
# in process, which is what makes the settings actually travel through the packet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AUTO_RUN="05044_auto_run_${CLICKHOUSE_DATABASE}"
OPT_OUT_RUN="05044_opt_out_run_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_uncompressed_cache_insert_src;
DROP TABLE IF EXISTS t_uncompressed_cache_insert_dst;
DROP TABLE IF EXISTS dist_uncompressed_cache_insert_src;
DROP TABLE IF EXISTS dist_uncompressed_cache_insert_dst;

CREATE TABLE t_uncompressed_cache_insert_src
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE t_uncompressed_cache_insert_dst
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE dist_uncompressed_cache_insert_src AS t_uncompressed_cache_insert_src
ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_uncompressed_cache_insert_src);

CREATE TABLE dist_uncompressed_cache_insert_dst AS t_uncompressed_cache_insert_dst
ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_uncompressed_cache_insert_dst);

INSERT INTO t_uncompressed_cache_insert_src SELECT number, repeat('x', 128) FROM numbers(32768);

SET log_queries = 1;
SET parallel_distributed_insert_select = 2;
SET prefer_localhost_replica = 0;
SET enable_automatic_use_uncompressed_cache = 1;

-- Control: the shard auto-enables the cache for its own insert-select.
INSERT INTO dist_uncompressed_cache_insert_dst
SELECT * FROM dist_uncompressed_cache_insert_src
SETTINGS log_comment = '${AUTO_RUN}';

SET use_uncompressed_cache = 0;

-- The explicit opt-out must reach the shard on this carrier as well.
INSERT INTO dist_uncompressed_cache_insert_dst
SELECT * FROM dist_uncompressed_cache_insert_src
SETTINGS log_comment = '${OPT_OUT_RUN}';

SELECT count() = 2 * 32768 FROM t_uncompressed_cache_insert_dst;

SYSTEM FLUSH LOGS query_log;

SELECT count() > 0, sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses']) > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${AUTO_RUN}';

-- The row count guards against the aggregate silently summing over no rows at all.
SELECT count() > 0, sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'])
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND is_initial_query = 0
  AND has(databases, currentDatabase())
  AND log_comment = '${OPT_OUT_RUN}';

DROP TABLE dist_uncompressed_cache_insert_src;
DROP TABLE dist_uncompressed_cache_insert_dst;
DROP TABLE t_uncompressed_cache_insert_src;
DROP TABLE t_uncompressed_cache_insert_dst;
"
