#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_projection_default_codec_restore"
backup_disk_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'backups'")
backup_path="${backup_disk_path%/}/${backup_name}"
metadata_path="${backup_path}/metadata/${CLICKHOUSE_DATABASE}/t_projection_default_codec_source.sql"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_projection_default_codec_source SYNC" >/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_projection_default_codec_restored SYNC" >/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_projection_default_codec_fresh SYNC" >/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_projection_default_codec_plain SYNC" >/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_projection_default_codec_copy SYNC" >/dev/null
    if [[ -n "$backup_name" && -d "$backup_path" ]]; then
        rm -rf -- "$backup_path"
    fi
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE t_projection_default_codec_source
    (
        k UInt64,
        x Float64 CODEC(NONE),
        PROJECTION p (x CODEC(Default)) AS (SELECT k, x ORDER BY k)
    ) ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'LZ4';
"

# T64 can be constructed with no type but cannot compress the part statistics,
# which use the default codec as an untyped byte stream.
if output=$(${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_projection_default_codec_fresh
    (k UInt64, x Float64 CODEC(NONE), PROJECTION p (x CODEC(Default)) AS (SELECT k, x ORDER BY k))
    ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'T64'" 2>&1); then
    echo 'fresh table with untyped T64 default unexpectedly succeeded' >&2
    exit 1
fi
if [[ "$output" != *BAD_ARGUMENTS* || "$output" != *"Cannot validate codec T64 without a column type"* ]]; then
    echo "fresh T64 default failed for an unexpected reason: $output" >&2
    exit 1
fi
echo 'untyped fresh default rejected'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_default_codec_fresh'"

if output=$(${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_projection_default_codec_plain (k UInt64 CODEC(NONE), x Float64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'T64'" 2>&1); then
    echo 'plain table with untyped T64 default unexpectedly succeeded' >&2
    exit 1
fi
if [[ "$output" != *BAD_ARGUMENTS* || "$output" != *"Cannot validate codec T64 without a column type"* ]]; then
    echo "plain T64 default failed for an unexpected reason: $output" >&2
    exit 1
fi
echo 'untyped plain default rejected'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_default_codec_plain'"

if output=$(${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_projection_default_codec_source MODIFY SETTING default_compression_codec = 'T64'" 2>&1); then
    echo 'ALTER to untyped T64 default unexpectedly succeeded' >&2
    exit 1
fi
if [[ "$output" != *BAD_ARGUMENTS* || "$output" != *"Cannot validate codec T64 without a column type"* ]]; then
    echo "ALTER to T64 default failed for an unexpected reason: $output" >&2
    exit 1
fi
echo 'untyped altered default rejected'

if output=$(${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_projection_default_codec_copy AS t_projection_default_codec_source
    ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'T64'" 2>&1); then
    echo 'copied table with untyped T64 default unexpectedly succeeded' >&2
    exit 1
fi
if [[ "$output" != *BAD_ARGUMENTS* || "$output" != *"Cannot validate codec T64 without a column type"* ]]; then
    echo "copied T64 default failed for an unexpected reason: $output" >&2
    exit 1
fi
echo 'untyped copied default rejected'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_default_codec_copy'"

${CLICKHOUSE_CLIENT} -q "BACKUP TABLE t_projection_default_codec_source TO Disk('backups', '${backup_name}') FORMAT Null"

test -f "$metadata_path"
grep -q "default_compression_codec = 'LZ4'" "$metadata_path"
sed -i "s/default_compression_codec = 'LZ4'/default_compression_codec = 'SZ3'/" "$metadata_path"

if output=$(${CLICKHOUSE_CLIENT} --multiquery --query "
    SET enable_sz3_codec = 1;
    RESTORE TABLE t_projection_default_codec_source AS t_projection_default_codec_restored
        FROM Disk('backups', '${backup_name}') FORMAT Null;
" 2>&1); then
    echo 'lossy RESTORE unexpectedly succeeded' >&2
    exit 1
fi
if [[ "$output" != *BAD_ARGUMENTS* || "$output" != *"Codec SZ3 is lossy"* ]]; then
    echo "lossy RESTORE failed for an unexpected reason: $output" >&2
    exit 1
fi
echo 'lossy restore rejected'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_default_codec_restored'"

sed -i "s/default_compression_codec = 'SZ3'/default_compression_codec = 'T64'/" "$metadata_path"
if output=$(${CLICKHOUSE_CLIENT} -q "
    RESTORE TABLE t_projection_default_codec_source AS t_projection_default_codec_restored
    FROM Disk('backups', '${backup_name}') FORMAT Null" 2>&1); then
    echo 'RESTORE with untyped T64 default unexpectedly succeeded' >&2
    exit 1
fi
if [[ "$output" != *BAD_ARGUMENTS* || "$output" != *"Cannot validate codec T64 without a column type"* ]]; then
    echo "RESTORE with T64 default failed for an unexpected reason: $output" >&2
    exit 1
fi
echo 'untyped restore default rejected'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_default_codec_restored'"

sed -i "s/default_compression_codec = 'T64'/default_compression_codec = 'LZ4'/" "$metadata_path"
${CLICKHOUSE_CLIENT} --multiquery --query "
    RESTORE TABLE t_projection_default_codec_source AS t_projection_default_codec_restored
        FROM Disk('backups', '${backup_name}') FORMAT Null;
    INSERT INTO t_projection_default_codec_restored (k, x) VALUES (1, 1.125);
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_projection_default_codec_restored' AND name = 'p' AND active"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM t_projection_default_codec_restored ORDER BY k"
