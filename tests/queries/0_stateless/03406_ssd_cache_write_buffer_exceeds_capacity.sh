#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ssd_cache` and `complex_key_ssd_cache` dictionaries whose `write_buffer_size` exceeds the maximum cache file capacity
# (`max_partitions_count * file_size`) used to spin forever in `insertCell` on the first flush. Now loading is rejected.
# Dictionary loading can be lazy, so `SYSTEM RELOAD DICTIONARY` forces it.

dir="${CLICKHOUSE_USER_FILES_UNIQUE:?}"
rm -rf "${dir:?}"
mkdir -p "$dir"
trap 'rm -rf "${dir:?}"' EXIT
# `clickhouse-local` allows dictionary files only inside its user files path, which is the current directory.
cd "$dir" || exit 1

$CLICKHOUSE_LOCAL --query "
CREATE DICTIONARY d_invalid_simple (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(NULL()) LIFETIME(1)
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 8192 MAX_PARTITIONS_COUNT 1 PATH '$dir/d_invalid_simple'));
SYSTEM RELOAD DICTIONARY d_invalid_simple;
" 2>&1 | grep -o -F -m1 'BAD_ARGUMENTS'

$CLICKHOUSE_LOCAL --query "
CREATE DICTIONARY d_invalid_complex (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(NULL()) LIFETIME(1)
LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 8192 MAX_PARTITIONS_COUNT 1 PATH '$dir/d_invalid_complex'));
SYSTEM RELOAD DICTIONARY d_invalid_complex;
" 2>&1 | grep -o -F -m1 'BAD_ARGUMENTS'

# Quoted zero values bypass the integer check of the DDL parser and reach the configuration parser shared with XML dictionaries.
# A zero `WRITE_BUFFER_SIZE` used to dereference a null buffer, and a zero `BLOCK_SIZE` used to divide by zero.
for layout in SSD_CACHE COMPLEX_KEY_SSD_CACHE; do
    $CLICKHOUSE_LOCAL --query "
    CREATE DICTIONARY d_zero_buffer_$layout (k UInt64, v String DEFAULT '') PRIMARY KEY k
    SOURCE(NULL()) LIFETIME(1)
    LAYOUT($layout(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE '0' MAX_PARTITIONS_COUNT 1 PATH '$dir/d_zero_buffer_$layout'));
    SYSTEM RELOAD DICTIONARY d_zero_buffer_$layout;
    " 2>&1 | grep -o -F -m1 'BAD_ARGUMENTS'

    $CLICKHOUSE_LOCAL --query "
    CREATE DICTIONARY d_zero_block_$layout (k UInt64, v String DEFAULT '') PRIMARY KEY k
    SOURCE(NULL()) LIFETIME(1)
    LAYOUT($layout(BLOCK_SIZE '0' FILE_SIZE 4096 WRITE_BUFFER_SIZE 4096 MAX_PARTITIONS_COUNT 1 PATH '$dir/d_zero_block_$layout'));
    SYSTEM RELOAD DICTIONARY d_zero_block_$layout;
    " 2>&1 | grep -o -F -m1 'BAD_ARGUMENTS'
done

# At the boundary (`MAX_PARTITIONS_COUNT 2`: capacity 8192 == `write_buffer_size`) the dictionaries load,
# and 100 KB of values pass through the cache, so the write buffer is flushed to the file many times.
$CLICKHOUSE_LOCAL --query "
CREATE TABLE src (k UInt64, v String) ENGINE = Memory;
INSERT INTO src SELECT number, toString(number) || ':' || repeat('x', 1000) FROM numbers(100);

CREATE DICTIONARY d_valid_simple (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(MIN 1000 MAX 1000)
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 8192 MAX_PARTITIONS_COUNT 2 PATH '$dir/d_valid_simple'));

CREATE DICTIONARY d_valid_complex (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(MIN 1000 MAX 1000)
LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 8192 MAX_PARTITIONS_COUNT 2 PATH '$dir/d_valid_complex'));

SYSTEM RELOAD DICTIONARY d_valid_simple;
SYSTEM RELOAD DICTIONARY d_valid_complex;

SELECT count() FROM numbers(100) WHERE dictGet('d_valid_simple', 'v', number) = toString(number) || ':' || repeat('x', 1000);
SELECT count() FROM numbers(100) WHERE dictGet('d_valid_complex', 'v', tuple(number)) = toString(number) || ':' || repeat('x', 1000);
SELECT count() FROM numbers(100) WHERE dictGet('d_valid_simple', 'v', number) = toString(number) || ':' || repeat('x', 1000);
SELECT count() FROM numbers(100) WHERE dictGet('d_valid_complex', 'v', tuple(number)) = toString(number) || ':' || repeat('x', 1000);
SELECT name, status FROM system.dictionaries WHERE name LIKE 'd_valid_%' ORDER BY name;
"
