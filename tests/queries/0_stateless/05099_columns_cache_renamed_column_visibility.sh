#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
# Cache entries are identified by the name the schema gives a column, not by the name the part
# stores it under. After `RENAME COLUMN a TO b` an old part still keeps the data as `a`, but
# `system.columns_cache` must report the entries as `b`, a user granted only `b` must see them,
# and a user granted only a freshly re-added `a` must not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_b="user_b_05099_${CLICKHOUSE_DATABASE}"
user_a="user_a_05099_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cc_renamed"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user_b}, ${user_a}"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_cc_renamed (id UInt64, a UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0
"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_cc_renamed SELECT number, number * 2 FROM numbers(10000)"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_cc_renamed RENAME COLUMN a TO b"

# The part still stores the column as `a`; reading `b` populates the cache.
$CLICKHOUSE_CLIENT -q "SELECT sum(b) FROM t_cc_renamed SETTINGS use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "SELECT sum(b) FROM t_cc_renamed SETTINGS use_columns_cache = 1"

echo 'cached columns of the table:'
$CLICKHOUSE_CLIENT -q "
SELECT column, sum(rows) FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cc_renamed'
GROUP BY column ORDER BY column
"

# A user granted only `b` sees the entries of `b`.
$CLICKHOUSE_CLIENT -q "CREATE USER ${user_b}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.columns_cache TO ${user_b}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_cc_renamed TO ${user_b}"

echo 'user granted b sees:'
$CLICKHOUSE_CLIENT --user "${user_b}" -q "
SELECT column FROM system.columns_cache
WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_cc_renamed'
GROUP BY column ORDER BY column
"

# The old name is reintroduced as a new column. A user granted only the new `a` must not learn
# anything about the entries of `b`, which the part stores under the name `a`.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_cc_renamed ADD COLUMN a UInt64 DEFAULT 42"
$CLICKHOUSE_CLIENT -q "SELECT sum(b) FROM t_cc_renamed SETTINGS use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "SELECT sum(b) FROM t_cc_renamed SETTINGS use_columns_cache = 1"

$CLICKHOUSE_CLIENT -q "CREATE USER ${user_a}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.columns_cache TO ${user_a}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_cc_renamed TO ${user_a}"

echo 'cached columns of the table after re-adding a:'
$CLICKHOUSE_CLIENT -q "
SELECT column, sum(rows) FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cc_renamed'
GROUP BY column ORDER BY column
"
echo 'user granted the new a sees:'
$CLICKHOUSE_CLIENT --user "${user_a}" -q "
SELECT count() FROM system.columns_cache
WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_cc_renamed'
"
echo 'user granted b still sees:'
$CLICKHOUSE_CLIENT --user "${user_b}" -q "
SELECT column FROM system.columns_cache
WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_cc_renamed'
GROUP BY column ORDER BY column
"

$CLICKHOUSE_CLIENT -q "DROP USER ${user_b}, ${user_a}"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_cc_renamed"
