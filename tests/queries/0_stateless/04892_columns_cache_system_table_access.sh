#!/usr/bin/env bash
# Test that system.columns_cache filters its rows by access rights, mirroring
# system.columns. A user without SHOW TABLES / SHOW COLUMNS on a table must not
# be able to learn its database, table, part, or column names or cached sizes
# from system.columns_cache once another query has warmed the cache.
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_04892_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_columns_cache_acl"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_columns_cache_acl (id UInt64, secret String)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0
"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_columns_cache_acl SELECT number, toString(number) FROM numbers(10000)"

# Warm the cache as a fully-privileged user. The query must read the columns
# (not just count()), otherwise nothing is deserialized into the cache.
$CLICKHOUSE_CLIENT -q "
SELECT sum(id), max(secret) FROM t_columns_cache_acl
SETTINGS use_columns_cache = 1,
         enable_writes_to_columns_cache = 1,
         enable_reads_from_columns_cache = 1
" > /dev/null

echo 'privileged sees entries:'
$CLICKHOUSE_CLIENT -q "
SELECT count() > 0 FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_acl'
"

# A user that may read system.columns_cache but has no privilege on the table
# must not see the table's cache entries.
$CLICKHOUSE_CLIENT -q "CREATE USER ${user}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.columns_cache TO ${user}"

echo 'unprivileged sees entries:'
$CLICKHOUSE_CLIENT --user "${user}" -q "
SELECT count() FROM system.columns_cache
WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_columns_cache_acl'
"

# Granting access to the table makes its entries visible again (SELECT implies SHOW).
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_columns_cache_acl TO ${user}"

echo 'after grant sees entries:'
$CLICKHOUSE_CLIENT --user "${user}" -q "
SELECT count() > 0 FROM system.columns_cache
WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_columns_cache_acl'
"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_columns_cache_acl"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
