#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/04812_clickhouse_disks_move_directory.xml"

base_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
# The disk-level `remove -r` cannot empty a directory holding a virtual child on
# `plain_rewritable`, so residue survives it. Own the backing directory outright to keep the
# test repeatable (`clickhouse-test --database=X` shares one CLICKHOUSE_TMP across runs).
rm -rf "$base_dir"
trap 'rm -rf "$base_dir"' EXIT
mkdir -p \
    "$base_dir/local/data" "$base_dir/local/metadata" \
    "$base_dir/plain_rewritable"
export TEST_DISK_04812_PATH="$(realpath "$base_dir/local/data")/"
export TEST_DISK_04812_METADATA_PATH="$(realpath "$base_dir/local/metadata")/"
export TEST_DISK_PLAIN_REWRITABLE_04812_PATH="$(realpath "$base_dir/plain_rewritable")/"

dir="$CLICKHOUSE_TEST_UNIQUE_NAME"

function disks()
{
    clickhouse-disks -C "$config" --disk "$disk" --query "$1" 2>/dev/null
}

# Renaming a directory must behave identically on the `local` and `plain_rewritable`
# metadata backends. `clickhouse-disks` always exits 0, so assert the disk state.
function run_move_test()
{
    local disk="$1"
    echo "# $disk"

    disks "remove -r $dir"
    disks "mkdir --parents $dir/src"

    # Renaming to a name that does not exist yet moves the directory: the target
    # appears and the source is gone.
    disks "cd $dir; move src dst"
    disks "list $dir"

    # The target may name a path whose parent already exists.
    disks "mkdir $dir/parent"
    disks "cd $dir; move dst parent/child"
    disks "list $dir"
    disks "list $dir/parent"

    disks "remove -r $dir"
}

# Moving to a path whose intermediate directories do not exist. Here the two backends
# legitimately diverge, so this is kept out of `run_move_test`: the `plain_rewritable`
# rename materializes the missing intermediates, a local rename fails with ENOENT.
function run_move_missing_parent_test()
{
    local disk="$1"
    # Own working directory: `remove -r` does not empty a directory that holds a
    # virtual child on `plain_rewritable`, so reusing `$dir` would carry residue in.
    local mp="${dir}_mp"
    echo "# missing parent: $disk"

    disks "remove -r $mp"
    disks "mkdir --parents $mp/src"

    disks "cd $mp; move src aa/bb/cc"
    # On `plain_rewritable` the source is gone and the moved directory is reachable at
    # the full nested path. On a local disk the move failed and the source is still there.
    disks "list $mp"
    disks "list $mp/aa/bb"

    disks "remove -r $mp"
}

run_move_test "test_disk_04812"
run_move_test "test_disk_plain_rewritable_04812"

run_move_missing_parent_test "test_disk_04812"
run_move_missing_parent_test "test_disk_plain_rewritable_04812"
