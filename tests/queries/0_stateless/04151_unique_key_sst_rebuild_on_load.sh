#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
#
# UNIQUE KEY: load-time dense-index lifecycle.
#
# 1. A part that reaches disk without its `unique_key_index.sst` (e.g. a freeze
#    taken before UK shipped, or a sidecar lost on restore) is repaired on load:
#    DETACH/ATTACH re-runs loadDataParts, which rebuilds the SST. The part stays
#    active and its data is fully readable.
# 2. Fail-closed contract: a non-empty UK part whose dense index cannot be
#    rebuilt (missing UK column / unreadable rows / no RocksDB) is detached as
#    broken instead of activated. The rebuild-failure path is covered by the
#    USE_ROCKSDB=0 gtest (writeDenseIndexOnInsert / rebuildIfMissing throw) and
#    the CORRUPTED_DATA gtests; reproducing it via stateless filesystem
#    corruption trips the earlier checksum-consistency check first.
#    TODO(unique-key): add a fault-injection stateless variant that loads the
#    columns cleanly but fails the UK rebuild, asserting system.detached_parts.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS uk_rebuild_load"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_unique_key = 1;
    CREATE TABLE uk_rebuild_load (id UInt64, v String)
    ENGINE = MergeTree
    UNIQUE KEY (id)
    ORDER BY (id)
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO uk_rebuild_load VALUES (10, 'a'), (20, 'b'), (30, 'c')"

DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")

echo "sst_present_before_detach"
[ -f "${DATA_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"

# Detach so the part files are quiescent, drop the SST sidecar, then reattach.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_rebuild_load"
rm -f "${DATA_PATH}unique_key_index.sst"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE uk_rebuild_load"

# Part survived load and the SST was rebuilt; data is intact.
echo "active_parts_after_attach"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active"

NEW_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")
echo "sst_present_after_attach"
[ -f "${NEW_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"

echo "rows_after_attach"
${CLICKHOUSE_CLIENT} --query "SELECT id, v FROM uk_rebuild_load ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE uk_rebuild_load"
