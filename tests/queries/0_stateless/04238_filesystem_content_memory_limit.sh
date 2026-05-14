#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Build a file that comfortably exceeds the per-query memory limit set below.
# 32 MiB is large enough to be a hard requirement when max_memory_usage = 1 MiB
# and tiny relative to any reasonable host, so the test never stresses the server.
mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
head -c 33554432 /dev/zero > "${CLICKHOUSE_USER_FILES_UNIQUE}/big.bin"

TEST_REL="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Selecting the file content must trip the memory tracker. Before reading directly
# into the column buffer, the file content was first staged into a `std::string`
# allocated through `operator new`, which counts allocations but never throws on
# the limit -- so a single large file could push a small `clickhouse-local`
# instance past its cgroup limit and into an OOM kill instead of a clean
# `MEMORY_LIMIT_EXCEEDED`.
$CLICKHOUSE_CLIENT --max_memory_usage=1048576 --query "
    SELECT length(content)
    FROM filesystem('${TEST_REL}')
    WHERE name = 'big.bin'
" 2>&1 | grep -c -F MEMORY_LIMIT_EXCEEDED

# Sanity check: with a generous limit, the same query succeeds and reports the
# correct file size, so the streaming path produces the same bytes as before.
$CLICKHOUSE_CLIENT --query "
    SELECT length(content)
    FROM filesystem('${TEST_REL}')
    WHERE name = 'big.bin'
"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
