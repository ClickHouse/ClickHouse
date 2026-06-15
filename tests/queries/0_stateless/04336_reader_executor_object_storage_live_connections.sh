#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
#
# no-fasttest        -- needs minio (the s3() / object-storage source path)
# no-random-settings -- the assertions read ReaderExecutor ProfileEvents, which
#                       random read-path settings would perturb
#
# StorageObjectStorageSource (the object-storage table function/engine) must honor
# reader_executor_use_long_connections the same way DiskObjectStorage::prepareRead does:
# with the executor on and long connections disabled, source reads take the stateless
# one-shot path and open NO long connection; with them enabled, a scan whose contiguous
# run exceeds the read window opens one. Regression guard for the path that always called
# needLongConnectionLimit regardless of the setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3="s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/04336.parquet', 'clickhouse', 'clickhouse', 'Parquet')"

# Incompressible columns (cityHash64), written as ONE Parquet row group, so each column
# chunk is a contiguous run far larger than the 8 MiB read window - large enough that the
# structural open rule (predicted reach > window) fires when long connections are enabled.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION $S3
    SELECT cityHash64(number) AS c1, cityHash64(number + 1) AS c2, cityHash64(number + 2) AS c3
    FROM numbers(4000000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100000000
"

SCAN="SELECT count() FROM $S3 WHERE NOT ignore(*) FORMAT Null"
OFF_ID="04336_off_${CLICKHOUSE_DATABASE}"
ON_ID="04336_on_${CLICKHOUSE_DATABASE}"

# Disabled: the executor must use stateless one-shot reads -> no long connection opened.
$CLICKHOUSE_CLIENT --use_reader_executor=1 --reader_executor_use_long_connections=0 \
    --query_id "$OFF_ID" --query "$SCAN"
# Enabled: the executor opens a long connection for the over-window column run.
$CLICKHOUSE_CLIENT --use_reader_executor=1 --reader_executor_use_long_connections=1 \
    --query_id "$ON_ID" --query "$SCAN"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Disabled -> no long connection opened or served; the executor still ran (one-shots).
#   expected: 1
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['LongConnectionOpened'] = 0
       AND ProfileEvents['LongConnectionHits'] = 0
       AND ProfileEvents['ReaderExecutorSourceRequests'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = '$OFF_ID'
"

# Enabled -> a long connection is opened.
#   expected: 1
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['LongConnectionOpened'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = '$ON_ID'
"
