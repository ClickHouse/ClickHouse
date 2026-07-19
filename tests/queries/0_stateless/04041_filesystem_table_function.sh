#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create test files in the per-test unique directory
mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}/subdir"
echo -n 'hello' > "${CLICKHOUSE_USER_FILES_UNIQUE}/a.txt"
echo -n 'world' > "${CLICKHOUSE_USER_FILES_UNIQUE}/b.txt"
echo -n 'nested' > "${CLICKHOUSE_USER_FILES_UNIQUE}/subdir/c.txt"

# Relative path inside user_files
TEST_REL="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# List files and check basic columns (name, type, is_symlink, depth)
$CLICKHOUSE_CLIENT --query "
    SELECT name, type, is_symlink, depth
    FROM filesystem('${TEST_REL}')
    WHERE name IN ('a.txt', 'b.txt', 'c.txt', 'subdir')
    ORDER BY name
"

# Check size column for regular files
$CLICKHOUSE_CLIENT --query "
    SELECT name, size
    FROM filesystem('${TEST_REL}')
    WHERE name IN ('a.txt', 'b.txt', 'c.txt')
    ORDER BY name
"

# Check content column
$CLICKHOUSE_CLIENT --query "
    SELECT name, content
    FROM filesystem('${TEST_REL}')
    WHERE name IN ('a.txt', 'b.txt', 'c.txt')
    ORDER BY name
"

# LIMIT works
$CLICKHOUSE_CLIENT --query "
    SELECT name
    FROM filesystem('${TEST_REL}')
    WHERE type = 'regular'
    ORDER BY name
    LIMIT 1
"

# Check that content is NULL for directories
$CLICKHOUSE_CLIENT --query "
    SELECT name, content IS NULL
    FROM filesystem('${TEST_REL}')
    WHERE name = 'subdir'
"

# Clean up
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
