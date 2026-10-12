#!/usr/bin/env bash

# Regression test: `local_fs_method=mmap` must not combine with
# `use_page_cache_for_local_disks`, because `CachedInMemoryReadBufferFromFile`
# drives its inner reader via `set(piece, piece_size)` + `seek(offset)`, and
# `MMapReadBufferFromFileWithCache` rejects seeks past `piece_size`. On
# `master` the mmap path of `createReadBufferFromFileBase` returned directly
# without page-cache wrapping; `DiskLocal::prepareRead` should preserve that.
#
# A file large enough to span multiple page-cache blocks (default 64 KiB)
# would have triggered `CANNOT_SEEK_THROUGH_FILE` before the fix.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/04266_page_cache_config.yaml"

> "${CONFIG_FILE}" echo "
page_cache_max_size: 134217728
"

# Use a row count that produces at least a few hundred KiB of data so reads
# span multiple 64 KiB page-cache blocks.
$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" \
    --use_page_cache_for_local_disks 1 \
    --local_filesystem_read_method mmap \
    --min_bytes_to_use_mmap_io 1 \
    --multiquery "
CREATE TABLE test (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192;
INSERT INTO test SELECT number FROM numbers(200000);
SELECT count() FROM test WHERE NOT ignore(x);
"

rm "${CONFIG_FILE}"
