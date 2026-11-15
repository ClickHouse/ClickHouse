#!/usr/bin/env bash
# Tags: replica, no-fasttest, no-shared-merge-tree
# no-fasttest: Mutation load can be slow
# no-shared-merge-tree -- have separate test for it
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
function wait_mutation_loaded()
{
    local table=$1 && shift
    local expr=$1 && shift

    for _ in {1..60}; do
        result=$($CLICKHOUSE_CLIENT --query "SELECT * FROM system.mutations WHERE table = '$table' AND database='$CLICKHOUSE_DATABASE'")
        if [[ $result == *"$expr"* ]]; then
            return 0
        fi
        sleep 0.1
    done

    echo "[$table] Cannot wait mutation $expr" >&2
    return 1
}

declare -A tables
tables["wrong_metadata"]="min_bytes_for_wide_part = 0"
tables["wrong_metadata_compact"]="min_bytes_for_wide_part = 10000000"

for table in "${!tables[@]}"; do
    settings="${tables[$table]}"

    $CLICKHOUSE_CLIENT --query="
        DROP TABLE IF EXISTS $table;

        CREATE TABLE $table(
            a UInt64,
            b UInt64,
            c UInt64
        )
        ENGINE ReplicatedMergeTree('/test/{database}/tables/$table', '1')
        ORDER BY tuple()
        SETTINGS $settings;

        INSERT INTO $table VALUES (1, 2, 3);
        SYSTEM STOP MERGES $table;

        -- { echoOn }
        SELECT 'ECHO_ALIGNMENT_FIX' FORMAT Null;

        ALTER TABLE $table RENAME COLUMN a TO a1, RENAME COLUMN b to b1 SETTINGS replication_alter_partitions_sync = 0;
    "

    wait_column "$table" "\`a1\` UInt64" || exit 2

    $CLICKHOUSE_CLIENT --query="
        -- { echoOn }
        SELECT 'ECHO_ALIGNMENT_FIX' FORMAT Null;

        SELECT * FROM $table ORDER BY a1 FORMAT JSONEachRow;
        INSERT INTO $table VALUES (4, 5, 6);
        SELECT * FROM $table ORDER BY a1 FORMAT JSONEachRow;

        ALTER TABLE $table RENAME COLUMN a1 TO b, RENAME COLUMN b1 to a SETTINGS replication_alter_partitions_sync = 0;
    "

    wait_mutation_loaded "$table" "b1 TO a" || exit 2

    $CLICKHOUSE_CLIENT --query="
        -- { echoOn }
        SELECT 'ECHO_ALIGNMENT_FIX' FORMAT Null;

        INSERT INTO $table VALUES (7, 8, 9);
        SELECT * FROM $table ORDER by a1 FORMAT JSONEachRow;
        SYSTEM START MERGES $table;
        SYSTEM SYNC REPLICA $table;
    "

    wait_for_all_mutations "$table"

    $CLICKHOUSE_CLIENT --query="
        -- { echoOn }
        SELECT 'ECHO_ALIGNMENT_FIX' FORMAT Null;

        SELECT * FROM $table order by a FORMAT JSONEachRow;
    "
done |& grep -v -F -x -e '-- { echoOn }' -e "        SELECT 'ECHO_ALIGNMENT_FIX' FORMAT Null;"
