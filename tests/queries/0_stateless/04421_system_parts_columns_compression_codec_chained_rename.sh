#!/usr/bin/env bash
# Tags: replica, no-fasttest, no-shared-merge-tree
# no-fasttest: Mutation load can be slow.
# no-shared-merge-tree: SharedMergeTree uses a different mutation execution path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

function wait_column()
{
    local table=$1 && shift
    local column=$1 && shift

    for _ in {1..60}; do
        result=$($CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE $table")
        if [[ $result == *"$column"* ]]; then
            return 0
        fi
        sleep 0.1
    done

    echo "[$table] Cannot wait for column to appear" >&2
    return 1
}

table="test_system_parts_columns_compression_codec_chained_rename"

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS $table SYNC;

    CREATE TABLE $table
    (
        p UInt8,
        a String CODEC(ZSTD(3))
    )
    ENGINE = ReplicatedMergeTree('/test/{database}/tables/$table', '1')
    ORDER BY p
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4';

    INSERT INTO $table VALUES (1, 'value');
    SYSTEM STOP MERGES $table;

    ALTER TABLE $table RENAME COLUMN a TO b SETTINGS replication_alter_partitions_sync = 0;
"

wait_column "$table" "\`b\` String" || exit 2

$CLICKHOUSE_CLIENT --query="
    ALTER TABLE $table RENAME COLUMN b TO c SETTINGS replication_alter_partitions_sync = 0;
"

wait_column "$table" "\`c\` String" || exit 2

$CLICKHOUSE_CLIENT --query="
    SYSTEM START MERGES $table;
    SYSTEM SYNC REPLICA $table;
"

wait_for_all_mutations "$table"

$CLICKHOUSE_CLIENT --query="
    SELECT
        column,
        compression_codec
    FROM system.parts_columns
    WHERE database = currentDatabase()
        AND table = '$table'
        AND active
        AND column = 'c';

    DROP TABLE $table SYNC;
"
