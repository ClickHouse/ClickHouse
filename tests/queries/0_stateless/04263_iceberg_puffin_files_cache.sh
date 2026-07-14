#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
# no-fasttest: depends on local iceberg fixture
# no-parallel: cache is system-wide and tests can affect each other in unexpected way
# no-parallel-replicas: profile events are not available on the second replica
# no-random-settings: we need to test the interaction of specific setting combinations

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PATH="${CURDIR}/data_minio/dv_puffin_warehouse/default/dv_puffin_source"

$CLICKHOUSE_LOCAL -q "
SYSTEM DROP PUFFIN_FILES_CACHE;

SELECT count(id)
FROM icebergLocal('${TABLE_PATH}')
SETTINGS use_puffin_files_cache = 1;

SELECT event, value
FROM system.events
WHERE event IN ('PuffinFilesCacheHits', 'PuffinFilesCacheMisses', 'PuffinFilesRead')
ORDER BY event;

SELECT count(id)
FROM icebergLocal('${TABLE_PATH}')
SETTINGS use_puffin_files_cache = 1;

SELECT event, value
FROM system.events
WHERE event IN ('PuffinFilesCacheHits', 'PuffinFilesCacheMisses', 'PuffinFilesRead')
ORDER BY event;

SYSTEM DROP PUFFIN_FILES_CACHE;

SELECT count(id)
FROM icebergLocal('${TABLE_PATH}')
SETTINGS use_puffin_files_cache = 1;

SELECT event, value
FROM system.events
WHERE event IN ('PuffinFilesCacheHits', 'PuffinFilesCacheMisses', 'PuffinFilesRead')
ORDER BY event;

SYSTEM DROP PUFFIN_FILES_CACHE;

SELECT count(id)
FROM icebergLocal('${TABLE_PATH}')
SETTINGS use_puffin_files_cache = 0;

SELECT event, value
FROM system.events
WHERE event IN ('PuffinFilesCacheHits', 'PuffinFilesCacheMisses', 'PuffinFilesRead')
ORDER BY event;

SELECT count(id)
FROM icebergLocal('${TABLE_PATH}')
SETTINGS use_puffin_files_cache = 0;

SELECT event, value
FROM system.events
WHERE event IN ('PuffinFilesCacheHits', 'PuffinFilesCacheMisses', 'PuffinFilesRead')
ORDER BY event;
"
