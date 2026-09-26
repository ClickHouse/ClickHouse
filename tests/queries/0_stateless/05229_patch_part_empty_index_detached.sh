#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, no-parallel
# no-replicated-database, no-shared-merge-tree: the test reloads the table and reads
#   `system.detached_parts`, which a replicated table recovers from another replica.
# no-parallel: `patch_part_index_write_empty` is server-global, so a concurrent lightweight
#   `UPDATE` would write a corrupted patch part too.

# A patch part carries the index of the parts it patches in `source_parts.dat`. An index without
# source parts belongs to an empty covering part alone: for a patch part that holds rows it means the
# file lost its content, which used to load clean and silently unapply an acknowledged update - and
# then let `clearUnusedPatchParts` delete the only copy of it, because an empty index reports data
# version 0. The part is detached as broken on load now, so the rows stay recoverable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_patch_empty_index SYNC;
    CREATE TABLE t_patch_empty_index (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO t_patch_empty_index SELECT number, number FROM numbers(1000);
"

# The failpoint is server-global, so it is cleared even if a query below fails.
trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT patch_part_index_write_empty"' EXIT

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT patch_part_index_write_empty"

${CLICKHOUSE_CLIENT} --query "
    SET enable_lightweight_update = 1;
    UPDATE t_patch_empty_index SET v = v + 1000 WHERE id < 500;
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT patch_part_index_write_empty"

# The update is acknowledged and visible: the in-memory index of the patch part is intact, only its
# file is not.
echo -n 'updated sum: '
${CLICKHOUSE_CLIENT} --query "SELECT sum(v) FROM t_patch_empty_index"

echo -n 'active patch parts: '
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_patch_empty_index'
      AND active AND startsWith(name, 'patch-')
"

${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_patch_empty_index SYNC"
# Loading the corrupted part logs the `CORRUPTED_DATA` exception it is detached for, and the test
# harness fails a test whose client writes to stderr.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "ATTACH TABLE t_patch_empty_index"

# Before the fix the patch part loaded as an index with no source parts: still active, applying to
# nothing, and eligible for cleanup.
echo -n 'active patch parts after reload: '
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_patch_empty_index'
      AND active AND startsWith(name, 'patch-')
"

echo -n 'detached patch parts: '
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), any(reason) FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 't_patch_empty_index'
      AND startsWith(name, 'broken')
"

# The update is not applied any more - but it is reported and its rows are in \`detached/\`, instead
# of being deleted by the cleanup of a patch that looks materialized everywhere.
echo -n 'sum after reload: '
${CLICKHOUSE_CLIENT} --query "SELECT sum(v) FROM t_patch_empty_index"

# An uncorrupted patch part still loads and still applies after a reload.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_patch_good_index SYNC;
    CREATE TABLE t_patch_good_index (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO t_patch_good_index SELECT number, number FROM numbers(1000);
    SET enable_lightweight_update = 1;
    UPDATE t_patch_good_index SET v = v + 1000 WHERE id < 500;
"

${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_patch_good_index SYNC"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_patch_good_index"

echo -n 'reloaded sum: '
${CLICKHOUSE_CLIENT} --query "SELECT sum(v) FROM t_patch_good_index"

echo -n 'reloaded detached parts: '
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 't_patch_good_index'
"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE t_patch_empty_index SYNC;
    DROP TABLE t_patch_good_index SYNC;
"
