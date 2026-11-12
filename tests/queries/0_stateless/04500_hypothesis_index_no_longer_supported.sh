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

# A full-definition ATTACH is validated like CREATE, so it is rejected too.
# An `Atomic` or `Replicated` database attaches a table with a full definition only with its UUID; `Ordinary` has none.
UUID_CLAUSE=""
if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
    UUID_CLAUSE="UUID '$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")'"
fi
$CLICKHOUSE_CLIENT --query "
    SET allow_suspicious_low_cardinality_types = 1, send_logs_level = 'fatal';
    ATTACH TABLE t_attach ${UUID_CLAUSE} (c0 LowCardinality(UInt128), INDEX i0 90 % c0 TYPE hypothesis, c1 Bool)
    ENGINE = MergeTree() PRIMARY KEY tuple();
" 2>&1 | grep -oE "no longer supported|ILLEGAL_INDEX"

# A table stored with a hypothesis index still loads. The table is stored in a clickhouse-local data
# directory, its stored definition is rewritten to carry one, and the next start loads it.
local_query() { $CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/04500_data" "$@"; }
local_query --query "
    SET allow_suspicious_low_cardinality_types = 1;
    CREATE TABLE t_attach (c0 LowCardinality(UInt128), INDEX i0 90 % c0 TYPE minmax, c1 Bool)
    ENGINE = MergeTree() PRIMARY KEY tuple();
"
$CLICKHOUSE_LOCAL --param_metadata="${CLICKHOUSE_TMP:?}/04500_data/metadata/default/t_attach.sql" --param_copy="${CLICKHOUSE_TMP:?}/04500_data.sql" --query "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
        SELECT replaceOne(raw_blob, 'TYPE minmax', 'TYPE hypothesis') FROM file({metadata:String}, 'RawBLOB');
    INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
local_query --query "
    SELECT count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_attach' AND type = 'hypothesis';
"

# The legacy hypothesis index is inert: it holds no data and cannot be recomputed. An attached
# table carrying it must stay fully usable (issue #89797). Before the fix, INSERT, DELETE
# (a mutation) and OPTIMIZE (a merge) all failed with ILLEGAL_INDEX and the table wedged.
local_query --query "INSERT INTO t_attach (c0, c1) SELECT number, number % 2 FROM numbers(10);"
local_query --query "INSERT INTO t_attach (c0, c1) SELECT number + 100, number % 2 FROM numbers(10);"
# Filtered query on the indexed column no longer throws building the index condition.
local_query --query "SELECT count() FROM t_attach WHERE c0 = 3;"
# Merge succeeds and collapses the parts.
local_query --query "OPTIMIZE TABLE t_attach FINAL;"
local_query --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_attach' AND active;"
# Mutation (lightweight delete) completes instead of looping forever.
local_query --query "DELETE FROM t_attach WHERE c1;"
local_query --query "SELECT count() FROM t_attach;"
# Heavyweight delete-to-empty goes through MergeTreeData::createEmptyPart, a separate schedule
# point for the index aggregator: it must skip the inert index too (before the fix this mutation
# looped forever with ILLEGAL_INDEX). --mutations_sync 1 surfaces a wedge as a failed query.
local_query --mutations_sync 1 --query "ALTER TABLE t_attach DELETE WHERE 1;"
local_query --query "SELECT count() FROM t_attach;"
# No mutation is stuck (numbering-independent: asserts the wedge is gone regardless of versions).
local_query --query "SELECT countIf(is_done = 0) FROM system.mutations WHERE database = currentDatabase() AND table = 't_attach';"
# An explicit MATERIALIZE INDEX cannot rebuild the dead index: it must be rejected up front with
# ILLEGAL_INDEX (not silently succeed as a no-op). Rejected synchronously so no mutation is queued.
local_query --mutations_sync 1 --query "SET send_logs_level = 'fatal'; ALTER TABLE t_attach MATERIALIZE INDEX i0;" 2>&1 | grep -oE "no longer supported|ILLEGAL_INDEX"
# The rejected MATERIALIZE INDEX queued no mutation (the table is not wedged by it).
local_query --query "SELECT countIf(is_done = 0) FROM system.mutations WHERE database = currentDatabase() AND table = 't_attach';"
# The user can still drop the dead index.
local_query --query "ALTER TABLE t_attach DROP INDEX i0;"
local_query --query "
    SELECT count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_attach' AND type = 'hypothesis';
"
rm -rf "${CLICKHOUSE_TMP:?}/04500_data" "${CLICKHOUSE_TMP:?}/04500_data.sql"
