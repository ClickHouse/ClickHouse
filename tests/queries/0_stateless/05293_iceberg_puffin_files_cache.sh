#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
# no-fasttest: depends on local iceberg fixture
# no-parallel: cache is process-wide inside one clickhouse-local invocation
# no-parallel-replicas: profile events are not available on the second replica
# no-random-settings: the test checks specific setting combinations

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CUR_DIR}/data_minio/dv_puffin_warehouse/default/dv_puffin_source"

$CLICKHOUSE_LOCAL -q "
SYSTEM DROP PUFFIN FILES CACHE;

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

SYSTEM DROP PUFFIN FILES CACHE;

SELECT count(id)
FROM icebergLocal('${TABLE_PATH}')
SETTINGS use_puffin_files_cache = 1;

SELECT event, value
FROM system.events
WHERE event IN ('PuffinFilesCacheHits', 'PuffinFilesCacheMisses', 'PuffinFilesRead')
ORDER BY event;

SYSTEM DROP PUFFIN FILES CACHE;

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
