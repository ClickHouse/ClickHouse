#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: clickhouse-disks, object storage disks

# An object storage disk cannot exchange two files atomically, so `EXCHANGE TABLES` (also used by `CREATE OR REPLACE`
# and `REPLACE TABLE`) moves the two metadata files in three steps and a crash can stop between them. The database
# writes a record of the exchange first. Every state such a crash can leave is built here from a real record, and
# loading the database again must finish the exchange.
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

# Fixed UUIDs make the metadata paths and contents the same on both disks, so one real record serves both.
dir="store/053/05340000-0000-0000-0000-000000000001/"
x="${dir}a.sql"
y="${dir}b.sql"
t="${x}.tmp_exchange"
record="${dir}.a.sql.exchange"
saved_record="$base_dir/record"

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
    echo "$1: [$(disks "ls --all $1" | LC_ALL=C sort | xargs)]"
}

function show_tables()
{
    local_query "SELECT _database, _table, sum(x) FROM merge(REGEXP('^db'), '.*') GROUP BY ALL ORDER BY ALL"
}

# Builds the state left by `ops` with the saved record, loads the database, and exchanges the tables back.
function check_state()
{
    echo "-- $disk: $1"
    disks "$2" > /dev/null
    disks "write $record" < "$saved_record" > /dev/null
    show_tables
    show_files "$dir"
    local_query "EXCHANGE TABLES db.a AND db.b"
}

# Writes the saved record to `$2`, as a crash before the first move leaves it, and loads the database: nothing is exchanged.
function check_state_before_moves()
{
    echo "-- $disk: $1"
    disks "write $2" < "$saved_record" > /dev/null
    show_tables
    show_files "$dir"
}

for disk in test_db_disk_plain_rewritable_05340 test_db_disk_local_metadata_05340; do
    local_query "
        CREATE DATABASE db UUID '05340000-0000-0000-0000-000000000001' ENGINE = Atomic SETTINGS disk = '$disk';
        CREATE TABLE db.a UUID '05340000-0000-0000-0000-00000000000a' (x UInt32) ENGINE = MergeTree ORDER BY x;
        INSERT INTO db.a VALUES (1), (2), (3);
        CREATE TABLE db.b UUID '05340000-0000-0000-0000-00000000000b' (x UInt32) ENGINE = MergeTree ORDER BY x;
        INSERT INTO db.b VALUES (10), (20);
        EXCHANGE TABLES db.a AND db.b;
        EXCHANGE TABLES db.a AND db.b;"
    echo "-- $disk: a finished exchange leaves no record"
    show_files "$dir"

    if [ "$disk" = test_db_disk_plain_rewritable_05340 ]; then
        echo "-- $disk: an exchange that fails while writing its record changes nothing, the next load removes what it left"
        local_query "SYSTEM ENABLE FAILPOINT plain_object_storage_fail_after_copy_on_file_move; EXCHANGE TABLES db.a AND db.b;" | grep -o 'FAULT_INJECTED' | head -n1
        show_files "$dir"
        disks "read ${record}.tmp" | head -c -1 > "$saved_record"
        [ "$(sed -n 1p "$saved_record")" = "exchange v1" ] && echo "header"
        [ "$(sed -n 2p "$saved_record")" = "$x" ] && [ "$(sed -n 3p "$saved_record")" = "$y" ] && echo "both paths"
        sed -n 4p "$saved_record" | grep -q "05340000-0000-0000-0000-00000000000b" && echo "new content of the first file"
        sed -n 5p "$saved_record" | grep -q "05340000-0000-0000-0000-00000000000a" && echo "new content of the second file"
        show_tables
        show_files "$dir"

        check_state_before_moves "no move done" "$record"
        # A move of `plain_rewritable` copies the file before it removes the source, so a crash can also stop there.
        check_state "first move copied" "copy $x $t"
        check_state "first move done" "move $x $t"
        check_state "second move copied" "move $x $t; copy $y $x"
        check_state "second move done" "move $x $t; move $y $x"
        check_state "third move copied" "move $x $t; move $y $x; copy $t $y"

        echo "-- $disk: a file that is not a record is left as is"
        printf 'hello' | disks "write ${dir}.c.sql.exchange" > /dev/null
        show_tables
        show_files "$dir"
        disks "remove ${dir}.c.sql.exchange" > /dev/null

        echo "-- $disk: an exchange between databases records itself in both directories"
        dir_1="store/053/05340000-0000-0000-0000-000000000002/"
        dir_2="store/053/05340000-0000-0000-0000-000000000003/"
        local_query "
            CREATE DATABASE db_1 UUID '05340000-0000-0000-0000-000000000002' ENGINE = Atomic SETTINGS disk = '$disk';
            CREATE DATABASE db_2 UUID '05340000-0000-0000-0000-000000000003' ENGINE = Atomic SETTINGS disk = '$disk';
            CREATE TABLE db_2.a UUID '05340000-0000-0000-0000-00000000000c' (x UInt32) ENGINE = MergeTree ORDER BY x;
            INSERT INTO db_2.a VALUES (1), (2), (3);
            CREATE TABLE db_1.b UUID '05340000-0000-0000-0000-00000000000d' (x UInt32) ENGINE = MergeTree ORDER BY x;
            INSERT INTO db_1.b VALUES (10), (20);
            SYSTEM ENABLE FAILPOINT plain_object_storage_fail_after_copy_on_file_move;
            EXCHANGE TABLES db_2.a AND db_1.b;" | grep -o 'FAULT_INJECTED' | head -n1
        show_files "$dir_1"
        show_files "$dir_2"
        disks "read ${dir_2}.a.sql.exchange.tmp" | head -c -1 > "$base_dir/record_2"
        disks "read ${dir_1}.b.sql.exchange.tmp" | head -c -1 > "$base_dir/record_1"
        cmp -s "$base_dir/record_1" "$base_dir/record_2" && echo "the two records are the same"
        show_tables
        show_files "$dir_1"
        show_files "$dir_2"

        # `db_1` is loaded before `db_2`, so it finds the copy of the record first.
        for state in "first move done:move ${dir_2}a.sql ${dir_2}a.sql.tmp_exchange" \
                     "second move done:move ${dir_2}a.sql ${dir_2}a.sql.tmp_exchange; move ${dir_1}b.sql ${dir_2}a.sql"; do
            echo "-- $disk: between databases, ${state%%:*}"
            disks "${state#*:}" > /dev/null
            disks "write ${dir_2}.a.sql.exchange" < "$base_dir/record_2" > /dev/null
            disks "write ${dir_1}.b.sql.exchange" < "$base_dir/record_1" > /dev/null
            show_tables
            show_files "$dir_1"
            show_files "$dir_2"
            local_query "EXCHANGE TABLES db_2.a AND db_1.b"
        done

        echo "-- $disk: a file with neither of the recorded contents stops the load, nothing is changed"
        disks "move $x $t" > /dev/null
        printf 'hello' | disks "write $y" > /dev/null
        disks "write $record" < "$saved_record" > /dev/null
        show_tables | grep -o 'CORRUPTED_DATA' | head -n1
        show_files "$dir"
    else
        check_state_before_moves "no move done" "$record"
        check_state_before_moves "a temporary record is left" "${record}.tmp"
        check_state "first move done" "move $x $t"
        check_state "second move done" "move $x $t; move $y $x"
    fi
done
