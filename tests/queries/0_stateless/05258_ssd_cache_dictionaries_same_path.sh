#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Two ssd_cache dictionaries (and two complex_key_ssd_cache ones) with the same PATH each return their own values.

path="$CLICKHOUSE_USER_FILES/${CLICKHOUSE_DATABASE}_same_path"
complex_path="$CLICKHOUSE_USER_FILES/${CLICKHOUSE_DATABASE}_same_path_complex"

$CLICKHOUSE_CLIENT --query="
    CREATE TABLE src_a (id UInt64, value UInt64) ENGINE = TinyLog;
    CREATE TABLE src_b (id UInt64, value UInt64) ENGINE = TinyLog;
    INSERT INTO src_a SELECT number, 1000 + number FROM numbers(500);
    INSERT INTO src_b SELECT number, 9000000 + number FROM numbers(500);

    CREATE DICTIONARY dict_a (id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src_a')) LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 65536 WRITE_BUFFER_SIZE 4096 PATH '$path'));
    CREATE DICTIONARY dict_b (id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src_b')) LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 65536 WRITE_BUFFER_SIZE 4096 PATH '$path'));

    CREATE TABLE src_c (key String, value UInt64) ENGINE = TinyLog;
    CREATE TABLE src_d (key String, value UInt64) ENGINE = TinyLog;
    INSERT INTO src_c SELECT toString(number), 1000 + number FROM numbers(500);
    INSERT INTO src_d SELECT toString(number), 9000000 + number FROM numbers(500);

    CREATE DICTIONARY dict_c (key String, value UInt64 DEFAULT 0) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'src_c')) LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 65536 WRITE_BUFFER_SIZE 4096 PATH '$complex_path'));
    CREATE DICTIONARY dict_d (key String, value UInt64 DEFAULT 0) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'src_d')) LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 65536 WRITE_BUFFER_SIZE 4096 PATH '$complex_path'));

    SELECT sum(dictGet('dict_a', 'value', number)) FROM numbers(500) FORMAT Null;
    SELECT sum(dictGet('dict_b', 'value', number)) FROM numbers(500) FORMAT Null;
    SELECT sum(dictGet('dict_c', 'value', tuple(toString(number)))) FROM numbers(500) FORMAT Null;
    SELECT sum(dictGet('dict_d', 'value', tuple(toString(number)))) FROM numbers(500) FORMAT Null;

    -- Only the caches can answer from here on.
    TRUNCATE TABLE src_a; TRUNCATE TABLE src_b; TRUNCATE TABLE src_c; TRUNCATE TABLE src_d;

    SELECT countIf(dictGet('dict_a', 'value', number) != 1000 + number) FROM numbers(500);
    SELECT countIf(dictGet('dict_b', 'value', number) != 9000000 + number) FROM numbers(500);
    SELECT countIf(dictGet('dict_c', 'value', tuple(toString(number))) != 1000 + number) FROM numbers(500);
    SELECT countIf(dictGet('dict_d', 'value', tuple(toString(number))) != 9000000 + number) FROM numbers(500);
"

# A symlink at a dictionary's cache file is kept, and the file it points to becomes the cache.
ln -s "${CLICKHOUSE_DATABASE}_link_target.bin" "$CLICKHOUSE_USER_FILES/${CLICKHOUSE_DATABASE}_link.bin"

$CLICKHOUSE_CLIENT --query="
    CREATE TABLE src_e (id UInt64, value UInt64) ENGINE = TinyLog;
    INSERT INTO src_e VALUES (1, 42);
    CREATE DICTIONARY dict_e (id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src_e')) LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 65536 WRITE_BUFFER_SIZE 4096 PATH '$CLICKHOUSE_USER_FILES/${CLICKHOUSE_DATABASE}_link'));
    SELECT dictGet('dict_e', 'value', toUInt64(1));
"

[ -L "$CLICKHOUSE_USER_FILES/${CLICKHOUSE_DATABASE}_link.bin" ] && echo "symlink kept"
wc -c < "$CLICKHOUSE_USER_FILES/${CLICKHOUSE_DATABASE}_link_target.bin"
