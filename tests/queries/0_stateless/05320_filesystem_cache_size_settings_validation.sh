#!/usr/bin/env bash
# Test that a cache disk rejects missing, conflicting or out-of-range size settings and an alignment above the segment size

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${dir}"

> "${dir}/config.yaml" echo "
filesystem_caches_path: '${dir}/caches/'
storage_configuration:
    disks:
        local_disk:
            type: object_storage
            object_storage_type: local
            path: '${dir}/local_disk/'
"

function create_cache_disk()
{
    local out
    if out=$($CLICKHOUSE_LOCAL --config-file "${dir}/config.yaml" --query "
        CREATE TABLE t (a Int32) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = cache, name = 'cache', path = 'cache', disk = 'local_disk', $1);
        SELECT 'disk_accepted'" 2>&1); then
        echo "$out" | grep -o -F -e 'disk_accepted'
    else
        echo "$out" | grep -o -F \
            -e 'must be defined in cache configuration' \
            -e 'cannot be specified at the same time' \
            -e '`max_size` cannot be 0' \
            -e 'must be in range (0, 1]' \
            -e 'must not exceed `max_file_segment_size`'
    fi
}

create_cache_disk "cache_policy = 'LRU'"
create_cache_disk "max_size = '1Mi', max_size_ratio_to_total_space = 0.5"
create_cache_disk "max_size = 0"
create_cache_disk "max_size_ratio_to_total_space = 0"
create_cache_disk "max_size_ratio_to_total_space = 1.5"
create_cache_disk "max_size_ratio_to_total_space = 1"
create_cache_disk "max_size = '10Mi', max_file_segment_size = '1Mi', boundary_alignment = '2Mi'"

rm -rf "${dir}"
