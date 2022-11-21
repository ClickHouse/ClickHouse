#!/usr/bin/env bash
# Tags: zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function query_with_retry
{
    retry=0
    until [ $retry -ge 5 ]
    do
        result=$($CLICKHOUSE_CLIENT $2 --query="$1" 2>&1)
        if [ "$?" == 0 ]; then
            echo -n "$result"
            return
        else
            retry=$(($retry + 1))
            sleep 3
        fi
    done
    echo "Query '$1' failed with '$result'"
}

function test_case
{
    engine="$1"
    partition_command="$2"
    tables=$(echo "$partition_command" | grep -E "load_parts_drop_partition_[a-z]*" -o)

    echo "Engine: $engine, Command: $partition_command"

    for table in $tables; do
        $CLICKHOUSE_CLIENT -n --query "
            DROP TABLE IF EXISTS $table SYNC;

            CREATE TABLE $table(id UInt64, val UInt64)
            ENGINE = $engine
            ORDER BY id PARTITION BY id;

            SYSTEM STOP MERGES $table;
        "

        $CLICKHOUSE_CLIENT --max_block_size=1 --max_insert_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=1 -n --query "
            INSERT INTO $table SELECT 1, number FROM numbers(100);

            SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = '$table' AND active;

            SYSTEM START MERGES $table;
        "

        query_with_retry "OPTIMIZE TABLE $table PARTITION 1 FINAL SETTINGS optimize_throw_if_noop = 1"
    done

    $CLICKHOUSE_CLIENT -n --query "
        SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'load_parts_drop_partition_src' AND active;
        SELECT count() > 0 FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'load_parts_drop_partition_src' AND NOT active;

        DETACH TABLE load_parts_drop_partition_src;
        ATTACH TABLE load_parts_drop_partition_src;

        SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'load_parts_drop_partition_src' AND active;
    "

    $CLICKHOUSE_CLIENT --query "$partition_command"

    $CLICKHOUSE_CLIENT -n --query "
        SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'load_parts_drop_partition_src' AND active;

        DETACH TABLE load_parts_drop_partition_src;
        ATTACH TABLE load_parts_drop_partition_src;

        SELECT count() FROM system.parts WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'load_parts_drop_partition_src';
    "

    for table in $tables; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE $table SYNC"
    done
}

declare -a engines=(
    "MergeTree"
    "ReplicatedMergeTree('/test/02482_load_parts_partitions/{database}/{table}', '1')"
)

for engine in "${engines[@]}"; do
    test_case "$engine" "ALTER TABLE load_parts_drop_partition_src DROP PARTITION 1"
    test_case "$engine" "ALTER TABLE load_parts_drop_partition_src DETACH PARTITION 1"
    test_case "$engine" "ALTER TABLE load_parts_drop_partition_src MOVE PARTITION 1 TO TABLE load_parts_drop_partition_dst"
    test_case "$engine" "TRUNCATE load_parts_drop_partition_src"
done
