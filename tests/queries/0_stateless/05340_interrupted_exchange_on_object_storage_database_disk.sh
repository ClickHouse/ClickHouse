#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: clickhouse-disks, object storage disks

# An object storage disk emulates `EXCHANGE TABLES` (also used by `CREATE OR REPLACE` and `REPLACE TABLE`) with three
# moves, so a crash can stop between them. Every state such a crash can leave is built here by hand, together with the
# journal record the exchange writes first; opening the disk again must finish the exchange. It also checks that a real
# exchange records itself first.
# https://github.com/ClickHouse/ClickHouse/issues/124833

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/05340_interrupted_exchange_on_object_storage_database_disk.xml"

base_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$base_dir"
trap 'rm -rf "$base_dir"' EXIT
mkdir -p "$base_dir/plain_rewritable" "$base_dir/local/data" "$base_dir/local/metadata"
export TEST_DISK_05340_PLAIN_REWRITABLE_PATH="$(realpath "$base_dir/plain_rewritable")/"
export TEST_DISK_05340_LOCAL_PATH="$(realpath "$base_dir/local/data")/"
export TEST_DISK_05340_LOCAL_METADATA_PATH="$(realpath "$base_dir/local/metadata")/"

journal="clickhouse_rename_exchange_journal"

function local_query()
{
    $CLICKHOUSE_LOCAL -C "$config" --path "$base_dir/local_path_$disk" -q "$1" 2>&1
}

function disks()
{
    clickhouse-disks -C "$config" --disk "$disk" --query "$1" 2>&1
}

function show_files()
{
    echo "journal: [$(disks "ls $journal" | xargs)], metadata: [$(disks "ls $metadata_dir" | xargs)]"
}

# Counts the journal records on the disk's own files, without opening the disk (opening it finishes and removes them).
function stored_records()
{
    if [ "$disk" = test_db_disk_plain_rewritable_05340 ]; then
        grep -rlF --binary-files=text 'rename exchange journal v1' "$base_dir/plain_rewritable" | wc -l
    else
        find "$base_dir/local/metadata/$journal" -type f 2>/dev/null | wc -l
    fi
}

# Builds the state left by `ops` and the record of the exchange of `a` and `b`, opens the database, and exchanges the
# tables back for the next state.
function check_state()
{
    echo "-- $disk: $1"
    printf 'rename exchange journal v1\n%s\n%s\n' "$x" "$y" | disks "$2; mkdir --parents $journal; write $journal/record" > /dev/null
    local_query "SELECT 'a', sum(x) FROM db.a; SELECT 'b', sum(x) FROM db.b; EXCHANGE TABLES db.a AND db.b;"
    show_files
}

for disk in test_db_disk_plain_rewritable_05340 test_db_disk_local_metadata_05340; do
    local_query "
        CREATE DATABASE db ENGINE = Atomic SETTINGS disk = '$disk';
        CREATE TABLE db.a (x UInt32) ENGINE = MergeTree ORDER BY x;
        INSERT INTO db.a VALUES (1), (2), (3);
        CREATE TABLE db.b (x UInt32) ENGINE = MergeTree ORDER BY x;
        INSERT INTO db.b VALUES (10), (20);
        EXCHANGE TABLES db.a AND db.b;
        EXCHANGE TABLES db.a AND db.b;"
    echo "-- $disk: a finished exchange leaves no record"
    echo "records on storage: $(stored_records)"
    metadata_dir=$($CLICKHOUSE_LOCAL -C "$config" --path "$base_dir/local_path_$disk" -q "SELECT metadata_path FROM system.databases WHERE name = 'db'")
    x="${metadata_dir}a.sql"
    y="${metadata_dir}b.sql"
    t="${x}_tmp_rename_exchange"
    show_files

    if [ "$disk" = test_db_disk_plain_rewritable_05340 ]; then
        # A move of `plain_rewritable` copies the file before it removes the source, so a crash can also stop there.
        check_state "first move copied" "copy $x $t"
        check_state "first move done" "move $x $t"
        check_state "second move copied" "move $x $t; copy $y $x"
        check_state "second move done" "move $x $t; move $y $x"
        check_state "third move copied" "move $x $t; move $y $x; copy $t $y"

        echo "-- $disk: an exchange that fails in its first move leaves its record, the next start discards it"
        local_query "SYSTEM ENABLE FAILPOINT plain_object_storage_fail_after_copy_on_file_move; EXCHANGE TABLES db.a AND db.b;" | grep -o 'FAULT_INJECTED' | head -n1
        echo "records on storage: $(stored_records)"
        record_file=$(grep -rlF --binary-files=text 'rename exchange journal v1' "$base_dir/plain_rewritable")
        [ "$(cat "$record_file")" = "$(printf 'rename exchange journal v1\n%s\n%s' "$x" "$y")" ] && echo "the record names both files"
        local_query "SELECT 'a', sum(x) FROM db.a; SELECT 'b', sum(x) FROM db.b;"
        echo "records on storage: $(stored_records)"
        show_files
    else
        check_state "first move done" "move $x $t"
        check_state "second move done" "move $x $t; move $y $x"
    fi

    echo "-- $disk: an entry that is not a record is left as is"
    printf 'hello' | disks "write $journal/hello" > /dev/null
    local_query "SELECT 'a', sum(x) FROM db.a; SELECT 'b', sum(x) FROM db.b;"
    show_files
done
