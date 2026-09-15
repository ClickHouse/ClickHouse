#!/usr/bin/env bash
# Tags: no-azure-blob-storage

set -e

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATABASE_1="${CLICKHOUSE_DATABASE}_1"
DATABASE_2="${CLICKHOUSE_DATABASE}_2"
DATABASE_3="${CLICKHOUSE_DATABASE}_3"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=0 -q "CREATE DATABASE ${DATABASE_1} ENGINE=Ordinary" 2>&1| grep -Fac "UNKNOWN_DATABASE_ENGINE"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DATABASE_1} ENGINE=Atomic"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DATABASE_2}"
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${DATABASE_3} ENGINE=Ordinary"

$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=0 -q "SHOW CREATE DATABASE ${DATABASE_1}"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=0 -q "SHOW CREATE DATABASE ${DATABASE_2}"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE ${DATABASE_3}"

uuid_db_1=`$CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.databases WHERE name='${DATABASE_1}'"`
uuid_db_2=`$CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.databases WHERE name='${DATABASE_2}'"`
$CLICKHOUSE_CLIENT -q "SELECT name,
                              engine,
                              splitByChar('/', data_path)[-2],
                              splitByChar('/', metadata_path)[-2] as uuid_path, ((splitByChar('/', metadata_path)[-3] as metadata) = substr(uuid_path, 1, 3)) OR metadata='metadata'
                              FROM system.databases WHERE name LIKE '${CLICKHOUSE_DATABASE}_%'" | sed "s/$uuid_db_1/00001114-1000-4000-8000-000000000001/g" | sed "s/$uuid_db_2/00001114-1000-4000-8000-000000000002/g"

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE ${DATABASE_1}.mt_tmp (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO ${DATABASE_1}.mt_tmp SELECT * FROM numbers(100);
CREATE TABLE ${DATABASE_3}.mt (n UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY (n % 5);
INSERT INTO ${DATABASE_3}.mt SELECT * FROM numbers(110);

RENAME TABLE ${DATABASE_1}.mt_tmp TO ${DATABASE_3}.mt_tmp; /* move from Atomic to Ordinary */
RENAME TABLE ${DATABASE_3}.mt TO ${DATABASE_1}.mt;         /* move from Ordinary to Atomic */
SELECT count() FROM ${DATABASE_1}.mt;
SELECT count() FROM ${DATABASE_3}.mt_tmp;

DROP DATABASE ${DATABASE_3};
"

explicit_uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DATABASE_2}.mt UUID '$explicit_uuid' (n UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY (n % 5)"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE ${DATABASE_2}.mt" | sed "s/$explicit_uuid/00001114-0000-4000-8000-000000000002/g"
$CLICKHOUSE_CLIENT -q "SELECT name, uuid, create_table_query FROM system.tables WHERE database='${DATABASE_2}'" | sed "s/$explicit_uuid/00001114-0000-4000-8000-000000000002/g"

# Keep each query in flight until the DDL finishes without depending on read parallelism
# or a fixed sleep duration. Opening the FIFO for writing waits for the server reader;
# its pipeline then holds the table while waiting for the row and EOF.
# Each FIFO has one reader, so the gated queries use a single replica.
gate_dir="${CLICKHOUSE_USER_FILES_UNIQUE}/atomic_gates"
mkdir -p "$gate_dir"
gate_pids=()
cleanup_gates()
{
    touch "$gate_dir/release"
    for pid in "${gate_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup_gates EXIT

start_gate()
{
    local name=$1
    mkfifo "$gate_dir/$name.tsv"
    (
        exec {gate_fd}>"$gate_dir/$name.tsv"
        touch "$gate_dir/$name.ready"
        while [ ! -e "$gate_dir/release" ]; do sleep 0.1; done
        echo 0 >&${gate_fd}
    ) &
    gate_pids+=("$!")
}

wait_for_gate()
{
    local name=$1
    local query_pid=$2
    for _ in $(seq 1 600); do
        if [ -e "$gate_dir/$name.ready" ]; then return; fi
        if ! kill -0 "$query_pid" 2>/dev/null; then wait "$query_pid"; return 1; fi
        sleep 0.1
    done
    echo "Timed out waiting for $name to open its FIFO" >&2
    return 1
}

start_gate select
$CLICKHOUSE_CLIENT --max_parallel_replicas=1 --input_format_parallel_parsing=0 -q "SELECT count(col), sum(col) FROM (SELECT n + gate AS col FROM ${DATABASE_1}.mt CROSS JOIN file('$gate_dir/select.tsv', TSV, 'gate UInt64') AS gate_input)" > "$gate_dir/select.out" &
select_pid=$!
wait_for_gate select "$select_pid"

start_gate insert
$CLICKHOUSE_CLIENT --max_parallel_replicas=1 --input_format_parallel_parsing=0 -q "INSERT INTO ${DATABASE_2}.mt SELECT number + gate FROM numbers(30) AS source CROSS JOIN file('$gate_dir/insert.tsv', TSV, 'gate UInt64') AS gate_input" &
insert_pid=$!
wait_for_gate insert "$insert_pid"

$CLICKHOUSE_CLIENT -m -q "
RENAME TABLE ${DATABASE_1}.mt TO ${DATABASE_1}.mt_tmp;
RENAME TABLE ${DATABASE_1}.mt_tmp TO ${DATABASE_2}.mt_tmp;
EXCHANGE TABLES ${DATABASE_2}.mt AND ${DATABASE_2}.mt_tmp;
RENAME TABLE ${DATABASE_2}.mt_tmp TO ${DATABASE_1}.mt;
EXCHANGE TABLES ${DATABASE_1}.mt AND ${DATABASE_2}.mt;
"

# Check that nothing changed
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${DATABASE_1}.mt"
uuid_mt1=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database='${DATABASE_1}' AND name='mt'")
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE ${DATABASE_1}.mt" | sed "s/$uuid_mt1/00001114-0000-4000-8000-000000000001/g"
$CLICKHOUSE_CLIENT --show_table_uuid_in_table_create_query_if_not_nil=1 -q "SHOW CREATE TABLE ${DATABASE_2}.mt" | sed "s/$explicit_uuid/00001114-0000-4000-8000-000000000002/g"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE ${DATABASE_1}.mt SETTINGS database_atomic_wait_for_drop_and_detach_synchronously=0;
CREATE TABLE ${DATABASE_1}.mt (s String) ENGINE=Log();
INSERT INTO ${DATABASE_1}.mt SELECT 's' || toString(number) FROM numbers(5);
SELECT count() FROM ${DATABASE_1}.mt
" # result: 5

start_gate tuple
$CLICKHOUSE_CLIENT --max_parallel_replicas=1 --input_format_parallel_parsing=0 -q "SELECT tuple(s, gate) FROM ${DATABASE_1}.mt CROSS JOIN file('$gate_dir/tuple.tsv', TSV, 'gate UInt64') AS gate_input" > /dev/null &
tuple_pid=$!
wait_for_gate tuple "$tuple_pid"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DATABASE_1}" --database_atomic_wait_for_drop_and_detach_synchronously=0 && echo "dropped"

touch "$gate_dir/release"
wait "$select_pid"
wait "$insert_pid"
wait "$tuple_pid"
wait
cat "$gate_dir/select.out"
trap - EXIT
rm -r "$gate_dir"

$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM ${DATABASE_2}.mt"    # result: 30, 435
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DATABASE_2}" --database_atomic_wait_for_drop_and_detach_synchronously=0
