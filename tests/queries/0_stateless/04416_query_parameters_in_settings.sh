#!/usr/bin/env bash
# Query parameters ({name:Type} substitutions) used as setting values, both in the
# SETTINGS clause of a query and in standalone SET queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SETTINGS clause of a SELECT.
$CLICKHOUSE_CLIENT --param_threads=4 -q "SELECT getSetting('max_threads') SETTINGS max_threads = {threads:UInt64}"

# Standalone SET query (parameter passed via --param_*).
$CLICKHOUSE_CLIENT --param_threads=7 -q "SET max_threads = {threads:UInt64}; SELECT getSetting('max_threads')"

# Standalone SET query (parameter defined with a preceding SET param_*).
$CLICKHOUSE_CLIENT -q "SET param_x = 5; SET max_threads = {x:UInt64}; SELECT getSetting('max_threads')"

# String-typed setting.
$CLICKHOUSE_CLIENT --param_comment='hello world' -q "SELECT getSetting('log_comment') SETTINGS log_comment = {comment:String}"

# Float-typed setting.
$CLICKHOUSE_CLIENT --param_ratio=0.5 -q "SELECT getSetting('max_bytes_ratio_before_external_group_by') SETTINGS max_bytes_ratio_before_external_group_by = {ratio:Float64}"

# Several parameters, mixed with a plain setting.
$CLICKHOUSE_CLIENT --param_a=3 --param_b=5 -q "SELECT getSetting('max_threads'), getSetting('max_block_size') SETTINGS max_threads = {a:UInt64}, max_block_size = {b:UInt64}, max_rows_to_read = 100"

# The same parameter in both the SETTINGS clause and the query body.
$CLICKHOUSE_CLIENT --param_v=4 -q "SELECT {v:UInt64} = getSetting('max_threads') SETTINGS max_threads = {v:UInt64}"

# Formatting round-trips the placeholder (formatQuery does not substitute parameters).
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('SELECT 1 SETTINGS max_threads = {threads:UInt64}')"

# Errors: a value that cannot be parsed as the declared type, and a missing parameter.
$CLICKHOUSE_CLIENT --param_bad='abc' -q "SELECT 1 SETTINGS max_threads = {bad:UInt64}" 2>&1 | grep -o "BAD_QUERY_PARAMETER" | head -n1
$CLICKHOUSE_CLIENT -q "SELECT 1 SETTINGS max_threads = {nonexistent:UInt64}" 2>&1 | grep -o "UNKNOWN_QUERY_PARAMETER" | head -n1
