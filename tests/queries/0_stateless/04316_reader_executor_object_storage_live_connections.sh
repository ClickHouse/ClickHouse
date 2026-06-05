#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
#
# no-fasttest        -- needs minio (the s3() / object-storage source path)
# no-random-settings -- the assertions read ReaderExecutor ProfileEvents, which
#                       random read-path settings would perturb
#
# StorageObjectStorageSource (the object-storage table function/engine) must honor
# reader_executor_use_live_connections the same way DiskObjectStorage::prepareRead
# does: with the executor on and live connections disabled, source reads take the
# stateless one-shot path and acquire no SourceBufferLimit slots / open no reusable
# live connections. Regression guard for the path that always called
# needBufferLimit regardless of the setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3="s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04316.parquet', 'clickhouse', 'clickhouse', 'Parquet')"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION $S3
    SELECT number AS c1, number AS c2, number AS c3, number AS c4, number AS c5
    FROM numbers(200000)
    SETTINGS s3_truncate_on_insert = 1
"

SCAN="SELECT count() FROM $S3 WHERE NOT ignore(*) FORMAT Null"
OFF_ID="04316_off_${CLICKHOUSE_DATABASE}"
ON_ID="04316_on_${CLICKHOUSE_DATABASE}"

# Disabled: the executor must use stateless one-shot reads.
$CLICKHOUSE_CLIENT --use_reader_executor=1 --reader_executor_use_live_connections=0 \
    --query_id "$OFF_ID" --query "$SCAN"
# Enabled: the executor may open and reuse live connections.
$CLICKHOUSE_CLIENT --use_reader_executor=1 --reader_executor_use_live_connections=1 \
    --query_id "$ON_ID" --query "$SCAN"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Disabled -> no live buffers, no slots; only stateless one-shot fallbacks.
#   expected: 1
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['LiveSourceBufferCreated'] = 0
       AND ProfileEvents['LiveSourceBufferHits'] = 0
       AND ProfileEvents['LiveSourceBufferFallbacks'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = '$OFF_ID'
"

# Enabled -> live connections are opened.
#   expected: 1
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['LiveSourceBufferCreated'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = '$ON_ID'
"
