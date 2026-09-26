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

# Query parameters work wherever setting values are parsed by ParserSetQuery. This includes the
# storage SETTINGS clause of CREATE TABLE.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS 04492_storage_setting"
$CLICKHOUSE_CLIENT --param_g=4096 -q "CREATE TABLE 04492_storage_setting (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = {g:UInt64}"
$CLICKHOUSE_CLIENT -q "SELECT create_table_query LIKE '%index_granularity = 4096%' FROM system.tables WHERE database = currentDatabase() AND name = '04492_storage_setting'"
$CLICKHOUSE_CLIENT -q "DROP TABLE 04492_storage_setting"

# It also covers the SETTINGS clause of BACKUP / RESTORE when that clause contains only core query
# settings: such a clause falls through to the trailing SETTINGS clause of a query (also parsed by
# ParserSetQuery), so the placeholder is accepted at parse time. Shown via EXPLAIN AST to avoid
# actually running a backup.
$CLICKHOUSE_CLIENT --param_threads=4 -q "EXPLAIN AST BACKUP TABLE system.one TO Disk('backups', 'b') SETTINGS max_threads = {threads:UInt64}" 2>&1 | grep -o "BackupQuery" | head -n1
$CLICKHOUSE_CLIENT --param_threads=4 -q "EXPLAIN AST RESTORE TABLE system.one FROM Disk('backups', 'b') SETTINGS max_threads = {threads:UInt64}" 2>&1 | grep -o "RestoreQuery" | head -n1

# A query parameter (a core query setting) cannot be combined with a backup/restore-specific setting
# (such as `async` or `base_backup`) in the same BACKUP / RESTORE SETTINGS clause: those settings are
# parsed by the separate ParserSetQuery::parseNameValuePair path, which does not understand the
# {name:Type} placeholder. Here `async = true` is consumed as a backup setting and the remainder
# `, max_threads = {threads:UInt64}` is left unparsed, so the query is rejected with a SYNTAX_ERROR at
# parse time rather than misbehaving. (This path is unchanged by this PR, so the rejection is identical
# with or without the feature.)
$CLICKHOUSE_CLIENT --param_threads=4 -q "BACKUP TABLE system.one TO Disk('backups', 'b') SETTINGS async = true, max_threads = {threads:UInt64}" 2>&1 | grep -o "SYNTAX_ERROR" | head -n1

# The boundary is the separate ParserSetQuery::parseNameValuePair path (used by CREATE NAMED
# COLLECTION, dictionaries, ...), which does not support query parameters: a placeholder there is
# rejected at parse time with a SYNTAX_ERROR rather than silently misbehaving.
$CLICKHOUSE_CLIENT --param_v='secret' -q "CREATE NAMED COLLECTION 04492_nc AS key1 = {v:String}" 2>&1 | grep -o "SYNTAX_ERROR" | head -n1
