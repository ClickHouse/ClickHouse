#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for issue #93132:
# DETACH+ATTACH of a table after DROP PARTITION on a patch partition produced a
# broken-on-start patch part because `createEmptyPart` used the table's metadata
# (no partition key, no patch system columns) when synthesizing the empty
# covering part. The empty part was therefore missing `partition.dat` and
# `source_parts.dat`, and re-loading it failed with `FILE_DOESNT_EXIST` (or,
# before commit `1316b57d986`, with `UNKNOWN_IDENTIFIER` while resolving
# `__patchPartitionID(_part, ...)`).

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_93132;
    CREATE TABLE t_93132 (c1 Bool, c2 String)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS enable_block_offset_column = 1, enable_block_number_column = 1;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_93132 (c1, c2) VALUES (1, 'a')"

# The lightweight UPDATE creates a patch part in a `patch-<hash>-all` partition.
${CLICKHOUSE_CLIENT} --query "
    SET apply_mutations_on_fly = 1, allow_experimental_lightweight_update = 1;
    UPDATE t_93132 SET c1 = 0, c2 = 'b' WHERE TRUE;
"

# Look up the patch partition id and drop it. Before the fix this created an
# empty covering part with table metadata (wrong) and led to a broken part on
# the next ATTACH.
PATCH_PARTITION_ID=$(${CLICKHOUSE_CLIENT} --query "
    SELECT any(partition_id) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_93132'
      AND active AND startsWith(partition_id, 'patch-')
")

if [ -z "$PATCH_PARTITION_ID" ]; then
    echo "FAIL: no patch partition created"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_93132 DROP PARTITION ID '$PATCH_PARTITION_ID'"

${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_93132 SYNC"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_93132"

# Before the fix, this returned a non-empty list with `broken-on-start_patch-...`.
echo "broken_parts: $(${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 't_93132'
      AND startsWith(name, 'broken')
")"

# The data row from the original insert (post-update) must still be readable.
echo -n "data: "
${CLICKHOUSE_CLIENT} --query "SELECT c1, c2 FROM t_93132 ORDER BY c2"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_93132"

