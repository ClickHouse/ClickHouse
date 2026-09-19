#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS projection_codec_backup_source SYNC;
        DROP TABLE IF EXISTS projection_codec_backup_restored SYNC;
        DROP TABLE IF EXISTS projection_codec_backup_existing SYNC;
    " >/dev/null
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE projection_codec_backup_source
    (
        x UInt64,
        y String,
        PROJECTION p
        (
            x CODEC(NONE),
            y CODEC(ZSTD(1))
        )
        AS
        (
            SELECT x, y ORDER BY x
        )
    )
    ENGINE = MergeTree
    ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO projection_codec_backup_source
    SELECT number, repeat('value', 20) FROM numbers(100000);

    BACKUP TABLE projection_codec_backup_source TO ${backup} FORMAT Null;

    RESTORE TABLE projection_codec_backup_source AS projection_codec_backup_restored
    FROM ${backup} FORMAT Null;
"

echo 'renamed restore metadata and data'
${CLICKHOUSE_CLIENT} --query "
    SELECT codecs
    FROM system.projections
    WHERE database = currentDatabase()
        AND table = 'projection_codec_backup_restored'
        AND name = 'p'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(x), sum(length(y))
    FROM projection_codec_backup_restored
    SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(column_data_compressed_bytes > column_data_uncompressed_bytes)
    FROM system.projection_parts_columns
    WHERE database = currentDatabase()
        AND table = 'projection_codec_backup_restored'
        AND active
        AND name = 'p'
        AND column = 'x'
"

# Exercise a mixed generation after restore: the restored part and a newly written part must both
# remain readable, and the merge must use the codec declaration reconstructed from backup metadata.
${CLICKHOUSE_CLIENT} --multiquery --query "
    INSERT INTO projection_codec_backup_restored
    SELECT number + 100000, repeat('new', 20) FROM numbers(100000);

    OPTIMIZE TABLE projection_codec_backup_restored FINAL;
"

echo 'renamed restore after merge'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(x), sum(length(y))
    FROM projection_codec_backup_restored
    SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(column_data_compressed_bytes > column_data_uncompressed_bytes)
    FROM system.projection_parts_columns
    WHERE database = currentDatabase()
        AND table = 'projection_codec_backup_restored'
        AND active
        AND name = 'p'
        AND column = 'x'
"
${CLICKHOUSE_CLIENT} --query "CHECK TABLE projection_codec_backup_restored SETTINGS check_query_single_value_result = 1 FORMAT Null"

# An existing destination can deliberately have a different codec for the same projection when the
# operator opts into restoring across table-definition differences. Restored projection parts must
# be decoded from their own frames; later parts and merges use destination metadata.
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE projection_codec_backup_existing
    (
        x UInt64,
        y String,
        PROJECTION p
        (
            x CODEC(Delta, ZSTD(1)),
            y CODEC(ZSTD(1))
        )
        AS
        (
            SELECT x, y ORDER BY x
        )
    )
    ENGINE = MergeTree
    ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES projection_codec_backup_existing;

    INSERT INTO projection_codec_backup_existing
    SELECT number + 200000, repeat('existing', 20) FROM numbers(100000);

    RESTORE TABLE projection_codec_backup_source AS projection_codec_backup_existing
    FROM ${backup}
    SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1
    FORMAT Null;
"

echo 'restore into different codec metadata'
${CLICKHOUSE_CLIENT} --query "
    SELECT codecs
    FROM system.projections
    WHERE database = currentDatabase()
        AND table = 'projection_codec_backup_existing'
        AND name = 'p'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(x), sum(length(y))
    FROM projection_codec_backup_existing
    SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(column_data_compressed_bytes > column_data_uncompressed_bytes),
        countIf(column_data_compressed_bytes < column_data_uncompressed_bytes)
    FROM system.projection_parts_columns
    WHERE database = currentDatabase()
        AND table = 'projection_codec_backup_existing'
        AND active
        AND name = 'p'
        AND column = 'x'
"

${CLICKHOUSE_CLIENT} --multiquery --query "
    SYSTEM START MERGES projection_codec_backup_existing;
    OPTIMIZE TABLE projection_codec_backup_existing FINAL;
"

echo 'restore into different codec after merge'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(x), sum(length(y))
    FROM projection_codec_backup_existing
    SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(column_data_compressed_bytes < column_data_uncompressed_bytes)
    FROM system.projection_parts_columns
    WHERE database = currentDatabase()
        AND table = 'projection_codec_backup_existing'
        AND active
        AND name = 'p'
        AND column = 'x'
"
${CLICKHOUSE_CLIENT} --query "CHECK TABLE projection_codec_backup_existing SETTINGS check_query_single_value_result = 1 FORMAT Null"
