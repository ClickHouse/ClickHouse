#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: local filesystem layout is prepared outside SQL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

query()
{
    ${CLICKHOUSE_CLIENT} --send_logs_level=none --query "$1"
}

get_disk_path()
{
    local disk_name="$1"
    local path
    path=$(query "SELECT path FROM system.disks WHERE name = '${disk_name}'")

    if [[ -z "$path" ]]
    then
        echo "Missing disk ${disk_name}. Check that storage_conf_04506.xml is installed." >&2
        exit 1
    fi

    echo "$path"
}

with_trailing_slash()
{
    local path="$1"

    if [[ "$path" == "/" || "$path" != *"/04506_alter_storage_policy_existing_disk_contents/"* ]]
    then
        echo "Unsafe disk path: $path" >&2
        exit 1
    fi

    if [[ "$path" == */ ]]
    then
        echo "$path"
    else
        echo "${path}/"
    fi
}

disk1_root=$(with_trailing_slash "$(get_disk_path disk1_04506)")
disk2_root=$(with_trailing_slash "$(get_disk_path disk2_04506)")

remove_test_root()
{
    local path="$1"
    if [[ "$path" != *"/04506_alter_storage_policy_existing_disk_contents/"* ]]
    then
        echo "Refusing to remove unexpected path: $path" >&2
        exit 1
    fi

    rm -rf "$path"
}

cleanup()
{
    query "DROP TABLE IF EXISTS t_04506_no_path SYNC"
    query "DROP TABLE IF EXISTS t_04506_safe_contents SYNC"
    query "DROP TABLE IF EXISTS t_04506_bad_version SYNC"
    query "DROP TABLE IF EXISTS t_04506_version_directory SYNC"
    query "DROP TABLE IF EXISTS t_04506_detached_file SYNC"
    query "DROP TABLE IF EXISTS t_04506_unknown_root SYNC"
    query "DROP TABLE IF EXISTS t_04506_temporary_file SYNC"
    query "DROP TABLE IF EXISTS t_04506_root_part SYNC"
    query "DROP TABLE IF EXISTS t_04506_detached_part SYNC"
    remove_test_root "$disk1_root"
    remove_test_root "$disk2_root"
}

trap cleanup EXIT
cleanup

create_table()
{
    local table="$1"
    query "
        CREATE TABLE ${table} (x UInt64)
        ENGINE = MergeTree
        ORDER BY x
        SETTINGS storage_policy = 'policy_04506_disk1'"
    query "INSERT INTO ${table} VALUES (1)"
}

table_data_path()
{
    local table="$1"
    query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '${table}'"
}

disk2_data_path()
{
    local data_path="$1"
    local data_path_suffix

    if [[ "$data_path" != "$disk1_root"* ]]
    then
        echo "Unexpected data path: $data_path" >&2
        exit 1
    fi

    data_path_suffix="${data_path#"$disk1_root"}"
    if [[ -z "$data_path_suffix" ]]
    then
        echo "Unexpected empty data path suffix: $data_path" >&2
        exit 1
    fi

    echo "${disk2_root}${data_path_suffix}"
}

expect_error()
{
    local query_text="$1"
    local pattern="$2"
    ${CLICKHOUSE_CLIENT} --send_logs_level=none --query "$query_text" 2>&1 | grep -m1 -oE "$pattern"
}

run_case()
{
    local table="$1"
    local setup_function="$2"
    local expected_error="${3:-}"

    create_table "$table"

    local data_path
    local disk2_path
    data_path=$(table_data_path "$table")
    disk2_path=$(disk2_data_path "$data_path")
    "$setup_function" "$disk2_path" "$data_path"

    local alter_query="ALTER TABLE ${table} MODIFY SETTING storage_policy = 'policy_04506_disk1_disk2'"
    if [[ -n "$expected_error" ]]
    then
        expect_error "$alter_query" "$expected_error"
    else
        query "$alter_query"
        query "SELECT sum(x) FROM ${table}"
    fi

    query "DROP TABLE ${table} SYNC"
    remove_test_root "$disk2_path"
}

setup_no_path()
{
    :
}

setup_safe_contents()
{
    local disk2_path="$1"

    mkdir -p \
        "${disk2_path}/detached/not_a_part" \
        "${disk2_path}/tmp_1_1_0" \
        "${disk2_path}/delete_tmp_all_0_0_0" \
        "${disk2_path}/tmp-fetch_1_1_0"
}

setup_bad_version()
{
    local disk2_path="$1"

    mkdir -p "$disk2_path"
    printf 255 > "${disk2_path}/format_version.txt"
}

setup_version_directory()
{
    local disk2_path="$1"

    mkdir -p "${disk2_path}/format_version.txt"
}

setup_detached_file()
{
    local disk2_path="$1"

    mkdir -p "$disk2_path"
    touch "${disk2_path}/detached"
}

setup_unknown_root()
{
    setup_safe_contents "$@"
    mkdir -p "${1}/not_a_part"
}

setup_temporary_file()
{
    setup_safe_contents "$@"
    touch "${1}/tmp_not_a_directory"
}

setup_root_part()
{
    setup_safe_contents "$@"
    mkdir -p "${1}/all_0_0_0"
}

setup_detached_part()
{
    setup_safe_contents "$@"
    mkdir -p "${1}/detached/all_0_0_0"
}

run_case t_04506_no_path setup_no_path
run_case t_04506_safe_contents setup_safe_contents
run_case t_04506_bad_version setup_bad_version "Version file"
run_case t_04506_version_directory setup_version_directory "Bad version file"
run_case t_04506_detached_file setup_detached_file "already contain data"
run_case t_04506_unknown_root setup_unknown_root "already contain data"
run_case t_04506_temporary_file setup_temporary_file "already contain data"
run_case t_04506_root_part setup_root_part "already contain data"
run_case t_04506_detached_part setup_detached_part "already contain data"
