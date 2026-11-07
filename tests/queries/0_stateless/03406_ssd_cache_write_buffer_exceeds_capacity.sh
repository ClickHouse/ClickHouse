#!/usr/bin/env bash
# Tags: no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_USER_FILES_UNIQUE:?}"
rm -rf "${dir:?}"
mkdir -p "$dir"
trap 'rm -rf "${dir:?}"' EXIT

# Test rejection when write_buffer_size exceeds file capacity (max_partitions_count * file_size)
# for both ssd_cache and complex_key_ssd_cache layouts.

$CLICKHOUSE_LOCAL -q "
CREATE DICTIONARY d_invalid_simple (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(NULL()) LIFETIME(1)
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 8192 MAX_PARTITIONS_COUNT 1 PATH '$dir/d_invalid_simple'));
" 2>&1 | grep -q "setting 'write_buffer_size' (8192) cannot be greater than maximum file capacity" && echo "ssd_cache rejected invalid buffer"

$CLICKHOUSE_LOCAL -q "
CREATE DICTIONARY d_invalid_complex (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(NULL()) LIFETIME(1)
LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 8192 MAX_PARTITIONS_COUNT 1 PATH '$dir/d_invalid_complex'));
" 2>&1 | grep -q "setting 'write_buffer_size' (8192) cannot be greater than maximum file capacity" && echo "complex_key_ssd_cache rejected invalid buffer"

# Verify that valid configuration at boundary (MAX_PARTITIONS_COUNT 2 -> capacity 8192 == write_buffer_size 8192) is accepted
$CLICKHOUSE_LOCAL -q "
CREATE DICTIONARY d_valid (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(NULL()) LIFETIME(1)
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 8192 MAX_PARTITIONS_COUNT 2 PATH '$dir/d_valid'));
SELECT 'valid capacity accepted';
"
