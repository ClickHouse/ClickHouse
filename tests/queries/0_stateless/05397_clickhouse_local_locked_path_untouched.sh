#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# A second clickhouse-local started in a directory locked by a running instance must fail
# before it touches the files in that directory: neither the spill files in the temporary
# storage nor the temporary files of the persistent storage for access entities may be removed.

data_dir="${CLICKHOUSE_TMP}/locked_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"
mkdir -p "$data_dir/tmp" "$data_dir/access"

touch "$data_dir/tmp/tmp_spill_file"
touch "$data_dir/access/00000000-0000-0000-0000-000000000001.tmp"
touch "$data_dir/access/need_rebuild_lists.mark"

# `flock` holds an exclusive lock on the status file while the second instance runs, like a running instance does.
flock -x "$data_dir/status" ${CLICKHOUSE_BINARY} local --path "$data_dir" --query "SELECT 1" 2>&1 \
    | grep -o -m1 "Another server instance in same directory is already running"

[ -f "$data_dir/tmp/tmp_spill_file" ] && echo "spill file kept"
[ -f "$data_dir/access/00000000-0000-0000-0000-000000000001.tmp" ] && echo "access entity temporary file kept"
[ -f "$data_dir/access/need_rebuild_lists.mark" ] && echo "rebuild mark kept"
ls "$data_dir/access" | grep -c '\.list$'

rm -rf "$data_dir"
