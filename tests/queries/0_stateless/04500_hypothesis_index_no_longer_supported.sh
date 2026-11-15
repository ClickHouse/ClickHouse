#!/usr/bin/env bash
# Regression coverage for issue #89797.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `hypothesis` index type was removed. A legacy stub allows ATTACH of old tables for
# compatibility but rejects every path that would create a new one. Each rejected path must
# throw ILLEGAL_INDEX (Code 127): assert both the message and the error code, so a path that
# starts throwing a different code (while keeping the text) is still caught.
# send_logs_level = 'fatal' suppresses the server-side log echo of the same exception, so each
# rejected path prints exactly one message + code pair.

# CREATE TABLE with the exact table shape from the issue is rejected.
$CLICKHOUSE_CLIENT --query "
    SET allow_suspicious_low_cardinality_types = 1, send_logs_level = 'fatal';
    CREATE TABLE t0 (c0 LowCardinality(UInt128), INDEX i0 90 % c0 TYPE hypothesis, c1 Bool, c2 Map(Int64, Int64))
    ENGINE = MergeTree() PRIMARY KEY tuple();
" 2>&1 | grep -oE "no longer supported|ILLEGAL_INDEX"

# A minimal hypothesis index is rejected too, independent of column type.
$CLICKHOUSE_CLIENT --query "
    SET send_logs_level = 'fatal';
    CREATE TABLE t_create (a UInt64, INDEX i0 90 % a TYPE hypothesis) ENGINE = MergeTree() PRIMARY KEY tuple();
" 2>&1 | grep -oE "no longer supported|ILLEGAL_INDEX"

# ALTER ADD INDEX and CREATE INDEX are rejected as well.
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_alter (a UInt64) ENGINE = MergeTree() PRIMARY KEY tuple();"
$CLICKHOUSE_CLIENT --query "SET send_logs_level = 'fatal'; ALTER TABLE t_alter ADD INDEX i0 90 % a TYPE hypothesis;" 2>&1 | grep -oE "no longer supported|ILLEGAL_INDEX"
$CLICKHOUSE_CLIENT --query "SET send_logs_level = 'fatal'; CREATE INDEX i0 ON t_alter (90 % a) TYPE hypothesis;" 2>&1 | grep -oE "no longer supported|ILLEGAL_INDEX"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_alter;"

# Full-definition ATTACH keeps the compatibility bypass: it forwards attach = true, so a table
# carrying a hypothesis index is NOT rejected and can still be recreated (the metadata shape
# #89797 depends on). Atomic databases require an explicit UUID for full-definition ATTACH.
uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --query "
    SET allow_suspicious_low_cardinality_types = 1, send_logs_level = 'fatal';
    ATTACH TABLE t_attach UUID '$uuid' (c0 LowCardinality(UInt128), INDEX i0 90 % c0 TYPE hypothesis, c1 Bool)
    ENGINE = MergeTree() PRIMARY KEY tuple();
"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_attach' AND type = 'hypothesis';
"

# The legacy hypothesis index is inert: it holds no data and cannot be recomputed. An attached
# table carrying it must stay fully usable (issue #89797). Before the fix, INSERT, DELETE
# (a mutation) and OPTIMIZE (a merge) all failed with ILLEGAL_INDEX and the table wedged.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_attach (c0, c1) SELECT number, number % 2 FROM numbers(10);"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_attach (c0, c1) SELECT number + 100, number % 2 FROM numbers(10);"
# Filtered query on the indexed column no longer throws building the index condition.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_attach WHERE c0 = 3;"
# Merge succeeds and collapses the parts.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_attach FINAL;"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_attach' AND active;"
# Mutation (lightweight delete) completes instead of looping forever.
$CLICKHOUSE_CLIENT --query "DELETE FROM t_attach WHERE c1;"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_attach;"
# Heavyweight delete-to-empty goes through MergeTreeData::createEmptyPart, a separate schedule
# point for the index aggregator: it must skip the inert index too (before the fix this mutation
# looped forever with ILLEGAL_INDEX). --mutations_sync 1 surfaces a wedge as a failed query.
$CLICKHOUSE_CLIENT --mutations_sync 1 --query "ALTER TABLE t_attach DELETE WHERE 1;"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_attach;"
# No mutation is stuck (numbering-independent: asserts the wedge is gone regardless of versions).
$CLICKHOUSE_CLIENT --query "SELECT countIf(is_done = 0) FROM system.mutations WHERE database = currentDatabase() AND table = 't_attach';"
# An explicit MATERIALIZE INDEX cannot rebuild the dead index: it must be rejected up front with
# ILLEGAL_INDEX (not silently succeed as a no-op). Rejected synchronously so no mutation is queued.
$CLICKHOUSE_CLIENT --mutations_sync 1 --query "SET send_logs_level = 'fatal'; ALTER TABLE t_attach MATERIALIZE INDEX i0;" 2>&1 | grep -oE "no longer supported|ILLEGAL_INDEX"
# The rejected MATERIALIZE INDEX queued no mutation (the table is not wedged by it).
$CLICKHOUSE_CLIENT --query "SELECT countIf(is_done = 0) FROM system.mutations WHERE database = currentDatabase() AND table = 't_attach';"
# The user can still drop the dead index.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_attach DROP INDEX i0;"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_attach' AND type = 'hypothesis';
"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_attach;"
