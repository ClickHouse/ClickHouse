#!/usr/bin/env bash
# Test coverage for DatabaseOverlay::attachTable, detachTable, loadStoredObjects,
# loadTableFromMetadata, waitTableStarted, and waitDatabaseStarted.
#
# The default database in clickhouse-local is a DatabaseOverlay (Atomic + Filesystem).
# A second invocation with the same --path triggers the load* / wait* startup methods.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/overlay_test_XXXXXX")
trap 'rm -rf "${DATA_DIR}"' EXIT

LOCAL="$CLICKHOUSE_LOCAL --path ${DATA_DIR}"

# ---------------------------------------------------------------------------
# Session 1: create and populate a table.
# ---------------------------------------------------------------------------
$LOCAL --query "
CREATE TABLE t_overlay (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_overlay SELECT number FROM numbers(5);
SELECT * FROM t_overlay ORDER BY x;
"

# ---------------------------------------------------------------------------
# Session 2 (same path): exercises loadStoredObjects / loadTableFromMetadata /
# waitTableStarted / waitDatabaseStarted in DatabaseOverlay because the
# Atomic sub-database must reload its persisted table metadata on startup.
# ---------------------------------------------------------------------------
$LOCAL --query "
SELECT * FROM t_overlay ORDER BY x;
"

# ---------------------------------------------------------------------------
# Session 3: DETACH TABLE (DatabaseOverlay::detachTable) then
#             ATTACH TABLE (DatabaseOverlay::attachTable).
# ---------------------------------------------------------------------------
$LOCAL --query "
DETACH TABLE t_overlay;
SELECT count() FROM system.tables WHERE name = 't_overlay' AND database = 'default';
ATTACH TABLE t_overlay;
SELECT * FROM t_overlay ORDER BY x;
"

# ---------------------------------------------------------------------------
# Session 4: DROP the table and verify it is gone.
# ---------------------------------------------------------------------------
$LOCAL --query "
DROP TABLE t_overlay;
SELECT count() FROM system.tables WHERE name = 't_overlay' AND database = 'default';
"
