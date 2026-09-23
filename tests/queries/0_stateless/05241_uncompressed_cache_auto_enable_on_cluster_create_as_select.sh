#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database
# Tag no-random-settings: the test asserts uncompressed cache profile events, which a randomized
# `use_uncompressed_cache` would distort.
# Tag no-object-storage: automatic mode never applies to parts on object storage.
# Tag no-replicated-database: the test runs `ON CLUSTER` queries through the distributed DDL queue.
#
# A distributed DDL query is another carrier of the settings of a secondary query: the initiator queues the
# query text together with its changed settings, and the worker clamps the default-valued
# `use_uncompressed_cache = 0` away from those settings before it replays the query text, whose own
# `SETTINGS enable_automatic_use_uncompressed_cache = 1` would then switch the automatic mode back on for the
# local `MergeTree` read of a `CREATE ... AS SELECT`. The initiator therefore has to resolve the opt-out in
# both the queued settings and the queued query text.
#
# The worker runs the queued query under the `initial_query_id` of the `ON CLUSTER` query, which is set
# explicitly here to tell the rows of this test apart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_uncompressed_cache_ddl_src;

CREATE TABLE t_uncompressed_cache_ddl_src
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_uncompressed_cache_ddl_src SELECT number, repeat('x', 128) FROM numbers(32768);
"

AUTO_RUN_1="05241_auto_run_1_$(random_str 10)"
AUTO_RUN_2="05241_auto_run_2_$(random_str 10)"
OPT_OUT_RUN="05241_opt_out_run_$(random_str 10)"

# Control: the automatic mode is enabled from the query text only, and the second run finds the cache warm.
$CLICKHOUSE_CLIENT --query_id "$AUTO_RUN_1" --query "
CREATE TABLE t_uncompressed_cache_ddl_dst_1 ON CLUSTER test_shard_localhost ENGINE = MergeTree ORDER BY id
AS SELECT * FROM t_uncompressed_cache_ddl_src
SETTINGS enable_automatic_use_uncompressed_cache = 1, max_threads = 1" > /dev/null

$CLICKHOUSE_CLIENT --query_id "$AUTO_RUN_2" --query "
CREATE TABLE t_uncompressed_cache_ddl_dst_2 ON CLUSTER test_shard_localhost ENGINE = MergeTree ORDER BY id
AS SELECT * FROM t_uncompressed_cache_ddl_src
SETTINGS enable_automatic_use_uncompressed_cache = 1, max_threads = 1" > /dev/null

# The session-level opt-out must win on the worker over the automatic mode replayed from the query text. It is
# set with `SET`: a default-valued setting passed as a client option does not reach the query as changed.
$CLICKHOUSE_CLIENT --query_id "$OPT_OUT_RUN" --query "
SET use_uncompressed_cache = 0;
CREATE TABLE t_uncompressed_cache_ddl_dst_3 ON CLUSTER test_shard_localhost ENGINE = MergeTree ORDER BY id
AS SELECT * FROM t_uncompressed_cache_ddl_src
SETTINGS enable_automatic_use_uncompressed_cache = 1, max_threads = 1" > /dev/null

$CLICKHOUSE_CLIENT --query "
SELECT count() FROM t_uncompressed_cache_ddl_dst_1;
SELECT count() FROM t_uncompressed_cache_ddl_dst_2;
SELECT count() FROM t_uncompressed_cache_ddl_dst_3;

SYSTEM FLUSH LOGS query_log;

-- The warm control run hits the cache on the worker.
SELECT count() > 0, sum(ProfileEvents['UncompressedCacheHits']) > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND query_id != '${AUTO_RUN_2}'
  AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${AUTO_RUN_2}');

-- The opt-out run does not touch the uncompressed cache on the worker at all.
SELECT count() > 0, sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'])
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND query_id != '${OPT_OUT_RUN}'
  AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${OPT_OUT_RUN}');

DROP TABLE t_uncompressed_cache_ddl_dst_1;
DROP TABLE t_uncompressed_cache_ddl_dst_2;
DROP TABLE t_uncompressed_cache_ddl_dst_3;
DROP TABLE t_uncompressed_cache_ddl_src;
"
