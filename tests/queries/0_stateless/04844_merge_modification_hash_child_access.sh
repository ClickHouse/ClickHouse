#!/usr/bin/env bash

# Regression test for `StorageMerge::getModificationHash` probing its source tables without checking
# the current user's `SELECT` access to them (AI-review thread on PR #108721). An actual `Merge` read
# enforces per-child `SELECT` in `getSelectedTables`, while the modification hash runs before any
# access check - probing each child storage directly meant a user who lost access to one child could
# still get a consistent-query-cache hit backed by that child's current hash, and (since the query-cache
# key folds only the user and role IDs, not the grants) keep reading previously cached rows from it.
# The hash must recurse through `computeTableModificationHashForConsistency`, which fails closed on a
# child the user cannot `SELECT` from. `system.tables.modification_hash` takes the same path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Users are server-wide, so make the name unique per run: concurrent runs (e.g. the flaky check) must
# not see each other's grants. The database name folded into the cached queries keeps the stored query
# texts unique per run too.
user="user_04844_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the flaky check's settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t1_04844 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t2_04844 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t1_04844 VALUES (1);
    INSERT INTO t2_04844 VALUES (2);
    CREATE TABLE m_04844 (x UInt64) ENGINE = Merge(${CLICKHOUSE_DATABASE}, '^t[12]_04844$');

    CREATE USER ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m_04844 TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1_04844 TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t2_04844 TO ${user};
"

# With SELECT on every source table the hash is reported and the consistent cache works: the first run
# is stored, so the sum over both sources can be served consistently.
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT 'fully granted hash reported', isNotNull(modification_hash) FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'm_04844'"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT sum(x), 'qc_04844_merge' FROM m_04844 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
$CLICKHOUSE_CLIENT -q "SELECT 'entry stored while fully granted', count() > 0 FROM system.query_cache WHERE query LIKE '%qc_04844_merge%' AND query LIKE '%${CLICKHOUSE_DATABASE}%' AND query NOT LIKE '%system.query_cache%'"

# Losing SELECT on one source must fail the hash closed - for `system.tables.modification_hash` and,
# crucially, for the cache probe: the entry stored above still exists under this user's key, but the
# read now skips the revoked source (`getSelectedTables` requires per-child access), so serving the
# cached sum would keep exposing rows from the revoked table. The re-run must report the fresh sum of
# the remaining source, not the cached one.
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t2_04844 FROM ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT 'hash fails closed after revoke', isNull(modification_hash) FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'm_04844'"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT sum(x), 'qc_04844_merge' FROM m_04844 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"

$CLICKHOUSE_CLIENT -q "
    DROP USER ${user};
    DROP TABLE m_04844;
    DROP TABLE t1_04844;
    DROP TABLE t2_04844;
"
