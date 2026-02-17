#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Regression test for a bug where failed merge transaction rollback left renamed parts
# on disk, causing intersecting parts on ATTACH TABLE.
# When transactions_enabled is set on a table in an Ordinary database (which doesn't
# support transactions), merges fail at commit (NOT_IMPLEMENTED). The merge result is
# already renamed from tmp to final name by renameParts(), so rollback must rename it
# back to a tmp prefix to avoid stale intersecting parts on disk.
# Based on: https://github.com/ClickHouse/ClickHouse/pull/96635#issuecomment-3886219842

db_name="${CLICKHOUSE_DATABASE}_ordinary_03915"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 \
    -q "CREATE DATABASE IF NOT EXISTS ${db_name} ENGINE=Ordinary"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${db_name}.test_intersect (x UInt64, s String DEFAULT 'x')
        ENGINE = MergeTree ORDER BY x
        SETTINGS old_parts_lifetime = 3600
"

# Seed transactions_enabled on the table by executing a transaction.
# This causes subsequent merges to use the transaction system, which
# fails at commit on Ordinary database (no UUID support).
$CLICKHOUSE_CLIENT --multiquery \
    -q "BEGIN TRANSACTION; INSERT INTO ${db_name}.test_intersect VALUES (1, 'seed'); COMMIT" 2>/dev/null ||:

# Reset table state so the seeded part is loaded cleanly
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${db_name}.test_intersect"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${db_name}.test_intersect"

data_dir=$($CLICKHOUSE_CLIENT -q "
    SELECT arrayJoin(data_paths) FROM system.tables
    WHERE database = '${db_name}' AND name = 'test_intersect'" | head -1)

# Insert several parts
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES ${db_name}.test_intersect"
for i in $(seq 2 6); do
    $CLICKHOUSE_CLIENT -q "INSERT INTO ${db_name}.test_intersect VALUES ($i, 'v$i')"
done
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES ${db_name}.test_intersect"

# Trigger merge that fails at transaction commit.
# Without the fix: merge result stays on disk with final part name.
# With the fix: merge result is renamed to tmp_broken_merge_ prefix.
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ${db_name}.test_intersect FINAL" 2>/dev/null ||:

# Remove the first (lowest block number) level-0 part from disk.
# This shifts the merge range for the next OPTIMIZE, potentially creating
# a merge remnant that intersects (rather than contains) the first one.
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${db_name}.test_intersect"
for d in "${data_dir}"all_*; do
    [ -d "$d" ] || continue
    n=$(basename "$d")
    if [[ $n =~ ^all_[0-9]+_[0-9]+_0$ ]]; then
        rm -rf "$d"
        break
    fi
done
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${db_name}.test_intersect"

# Insert a new part to extend the block range
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES ${db_name}.test_intersect"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db_name}.test_intersect VALUES (9999, 'extra')"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES ${db_name}.test_intersect"

# Second merge that may overlap with the first remnant
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ${db_name}.test_intersect FINAL" 2>/dev/null ||:

# DETACH and ATTACH should not produce a LOGICAL_ERROR about intersecting parts
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${db_name}.test_intersect"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${db_name}.test_intersect"

# Verify the table is accessible and has data
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM ${db_name}.test_intersect"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db_name}"
