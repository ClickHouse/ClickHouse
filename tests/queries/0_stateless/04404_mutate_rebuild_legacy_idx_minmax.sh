#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# `no-fasttest`: local-disk part-file surgery is not reliably available on the Fast test macOS runner.
# `no-object-storage` / `no-shared-merge-tree` / `no-replicated-database`: the fixture edits real
# local part files and relies on ATTACH recomputing `checksums.txt` from them.
# `no-random-merge-tree-settings`: the fixture targets a standalone index file at a fixed granule
# count; the settings it needs are pinned in the CREATE below.
#
# Rebuild-path counterpart of 04402: when a mutation recomputes the index and writes a fresh
# `.idx2`, the legacy `.idx` must be neither hardlinked forward nor left in the inherited
# checksums. It used to leak dead beside the fresh file with a stale checksum. Issue #109595.
#
# Legacy shape fabricated as in 04402, then the indexed column is updated to force the rebuild.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_rebuild_minmax SYNC"

# v = k is monotone, so the minmax index over v prunes a point query to a
# single granule. `index_granularity` = 100 over 2000 rows gives 20 granules.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_rebuild_minmax
(
    k UInt64,
    v UInt64,
    INDEX mm_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         packed_skip_index_max_bytes = 0,
         columns_and_secondary_indices_sizes_lazy_calculation = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_rebuild_minmax (k, v) SELECT number, number FROM numbers(2000)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_rebuild_minmax FINAL"

# Detach so we can rewrite the part files, then downgrade the minmax index to
# the legacy ".idx" layout.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_rebuild_minmax DETACH PARTITION tuple() SETTINGS mutations_sync = 2"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_rebuild_minmax'")
PART_DIR=$(find "${DATA_PATH}detached" -maxdepth 1 -type d -name 'all_*' | head -1)

mv "${PART_DIR}/skp_idx_mm_v.idx2" "${PART_DIR}/skp_idx_mm_v.idx"
rm -f "${PART_DIR}/checksums.txt"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_rebuild_minmax ATTACH PARTITION tuple()"

# Sanity: the legacy ".idx" index is recognized and prunes to one granule.
echo "before:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_rebuild_minmax WHERE v = 42) WHERE explain ILIKE '%Granules: 1/20%'"

# Rebuild mutation: `ALTER UPDATE` of the indexed column v recomputes mm_v. The
# writer produces a fresh ".idx2". Before the fix the legacy ".idx" was
# hardlinked into the new part (leaked dead) with a stale checksum entry.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_rebuild_minmax UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2"

# The new ACTIVE part must carry the freshly written ".idx2" and NOT the stale
# legacy ".idx": the rebuild path must strip/skip it, not hardlink it. Resolve
# the active part directory via `system.parts` (the mutation leaves the old parts
# inactive but still on disk, so a plain directory scan would pick the wrong one).
NEW_PART_DIR=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_rebuild_minmax' AND active ORDER BY name LIMIT 1")
echo "legacy_idx_leaked:"
if ls "${NEW_PART_DIR}/skp_idx_mm_v.idx" >/dev/null 2>&1; then echo 1; else echo 0; fi
echo "current_idx2_present:"
if ls "${NEW_PART_DIR}/skp_idx_mm_v.idx2" >/dev/null 2>&1; then echo 1; else echo 0; fi

${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_rebuild_minmax SETTINGS check_query_single_value_result = 1"

# The recomputed index must still prune to one granule.
echo "after:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_rebuild_minmax WHERE v = 42) WHERE explain ILIKE '%Granules: 1/20%'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_rebuild_minmax WHERE v = 42"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_rebuild_minmax SYNC"
