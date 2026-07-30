#!/usr/bin/env bash
# A table must not be creatable unless its dropped-metadata name
# metadata_dropped/{db}.{table}.{uuid}.sql can exist. Past the point where the escaped
# database name alone fills that budget the limit is 0, so the CREATE is rejected up front
# instead of succeeding and leaving a table that cannot be dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pad the unique per-test database name out to an exact escaped length. CLICKHOUSE_DATABASE is
# test_<8 lowercase letters and digits>, so every byte is a word character and the escaped
# length equals the character count. The unique prefix is what keeps this parallel-safe.
pad() { printf '%*s' "$1" '' | tr ' ' d; }
# 211 is the largest escaped database name that still leaves room for a 2-character table name.
db_ok="${CLICKHOUSE_DATABASE}$(pad $((211 - ${#CLICKHOUSE_DATABASE})))"
# 214 is the first escaped length whose prefix alone exceeds the limit.
db_too_long="${CLICKHOUSE_DATABASE}$(pad $((214 - ${#CLICKHOUSE_DATABASE})))"

# When using s3_plain_rewriteable as a db disk, minio doesn't allow the path segment to have
# more than 255 characters, and these database names produce segments close to that limit.
# Refer: https://github.com/minio/minio/blob/ddd9a84cd769e6bed67f5fe860f8f3c7527a6971/cmd/xl-storage.go#L154-L167
use_s3_plain_rewriteable_as_db_disk=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name='disk_db_remote' AND type = 'ObjectStorage' AND object_storage_type='S3' AND metadata_type='PlainRewritable'" | tr -d '[:space:]')
if [ "$use_s3_plain_rewriteable_as_db_disk" == "0" ]; then
    # Just under the boundary: still allowed, and still droppable.
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$db_ok\` ENGINE = Atomic"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_ok\`.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
    echo "created"
    $CLICKHOUSE_CLIENT -q "DROP TABLE \`$db_ok\`.t0"
    echo "dropped"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$db_ok\`"

    # Past the boundary: rejected at CREATE, so no undroppable table is left behind.
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$db_too_long\` ENGINE = Atomic"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE \`$db_too_long\`.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()" 2>&1 | grep -o -m 1 'ARGUMENT_OUT_OF_BOUND'
    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$db_too_long\`"
else
    echo "created"
    echo "dropped"
    echo "ARGUMENT_OUT_OF_BOUND"
fi
