#!/usr/bin/env bash
# Tags: no-random-settings, no-s3-storage, no-azure-blob-storage
# Tag no-random-settings: the test asserts uncompressed cache profile events, which a randomized
# `use_uncompressed_cache` would distort.
#
# An explicit `use_uncompressed_cache = 0` coming from a settings profile must win over
# `enable_automatic_use_uncompressed_cache = 1`, exactly like an override in the query text.
#
# Settings profiles are server-global objects, so their names carry the database name to keep
# concurrent runs of this test (e.g. the flaky check) from dropping each other's profiles.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE_AUTO="profile_04612_auto_${CLICKHOUSE_DATABASE}"
PROFILE_OPT_OUT="profile_04612_opt_out_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP SETTINGS PROFILE IF EXISTS ${PROFILE_AUTO}, ${PROFILE_OPT_OUT};
CREATE SETTINGS PROFILE ${PROFILE_AUTO} SETTINGS enable_automatic_use_uncompressed_cache = 1;
CREATE SETTINGS PROFILE ${PROFILE_OPT_OUT} SETTINGS enable_automatic_use_uncompressed_cache = 1, use_uncompressed_cache = 0;

DROP TABLE IF EXISTS t_uncompressed_cache_profile;
CREATE TABLE t_uncompressed_cache_profile
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_uncompressed_cache_profile SELECT number, repeat('x', 128) FROM numbers(32768);

SET log_queries = 1;

-- Control: the automatic mode applied from a profile enables the cache when nothing opts out.
SET profile = '${PROFILE_AUTO}';

SELECT sum(length(payload)) = 32768 * 128 FROM t_uncompressed_cache_profile
SETTINGS max_threads = 1, log_comment = '04612_uncompressed_cache_profile_auto_run_1';

SELECT sum(length(payload)) = 32768 * 128 FROM t_uncompressed_cache_profile
SETTINGS max_threads = 1, log_comment = '04612_uncompressed_cache_profile_auto_run_2';

-- The explicit opt-out from the profile must be honored even though the cache is already warm.
SET profile = '${PROFILE_OPT_OUT}';

SELECT sum(length(payload)) = 32768 * 128 FROM t_uncompressed_cache_profile
SETTINGS max_threads = 1, log_comment = '04612_uncompressed_cache_profile_opt_out_run';

SYSTEM FLUSH LOGS query_log;

-- The warm control run hits the cache.
SELECT ProfileEvents['UncompressedCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04612_uncompressed_cache_profile_auto_run_2'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- The opt-out run does not touch the uncompressed cache at all.
SELECT ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04612_uncompressed_cache_profile_opt_out_run'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_uncompressed_cache_profile;
DROP SETTINGS PROFILE ${PROFILE_AUTO}, ${PROFILE_OPT_OUT};
"
