#!/usr/bin/env bash
# Test coverage for DatabaseOverlay::getDetachedTablesIterator.
#
# The default database in clickhouse-local is a DatabaseOverlay (Atomic + Filesystem).
# Previously, DatabaseOverlay did not implement getDetachedTablesIterator, causing
# system.detached_tables to silently hide tables detached from a DatabaseOverlay
# database instead of showing the correct data.
# This test verifies detached tables in a DatabaseOverlay database are visible.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/overlay_detached_test_XXXXXX")
trap 'rm -rf "${DATA_DIR}"' EXIT
LOCAL="$CLICKHOUSE_LOCAL --path ${DATA_DIR}"

# Single session: create a table, detach it, and verify it appears in
# system.detached_tables (this is the core bug fix being tested).
# Then re-attach it and verify it disappears from system.detached_tables.
$LOCAL --query "
CREATE TABLE t_overlay_detached (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_overlay_detached SELECT number FROM numbers(3);
DETACH TABLE t_overlay_detached;
SELECT database, table FROM system.detached_tables WHERE table = 't_overlay_detached';
ATTACH TABLE t_overlay_detached;
SELECT count() FROM system.detached_tables WHERE table = 't_overlay_detached';
SELECT * FROM t_overlay_detached ORDER BY x;
"
