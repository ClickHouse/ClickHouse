#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In clickhouse-local the default database overlays Filesystem and URL databases, which probe the filesystem for a
# file named like the table. A name longer than NAME_MAX made `fs::exists` throw `std::filesystem_error`
# ("File name too long", code 1001) instead of reporting an unknown table or dictionary.
# Found by json_ast_sql_execution_fuzzer.
long_name=$(printf 'a%.0s' $(seq 1 300))
${CLICKHOUSE_LOCAL} --query "SELECT * FROM ${long_name}" 2>&1 | grep -o 'UNKNOWN_TABLE' | head -1
${CLICKHOUSE_LOCAL} --query "SELECT dictGetFloat64('${long_name}', 'x', 1)" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1
${CLICKHOUSE_LOCAL} --query "EXISTS TABLE ${long_name}"
