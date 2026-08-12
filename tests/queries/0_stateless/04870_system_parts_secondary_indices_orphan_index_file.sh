#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# `system.parts.secondary_indices_materialized` must count only index data the
# part actually owns. A part in the released-bug shape of #109595 (see
# 04427_mutate_some_columns_drop_index_corrupted_idx) carries `skp_idx_<name>.*`
# files in its directory while `checksums.txt` has no entry for them: the index
# was dropped and re-added, so it is not materialized in that part until
# `ALTER TABLE ... MATERIALIZE INDEX` rebuilds it. Reporting it as materialized
# because the loose file happens to exist would make the column lie about
# exactly those legacy parts.
#
# no-fasttest: local-disk part-file surgery (as in 04427).
# no-object-storage/-shared/-replicated: relies on the local on-disk file layout.
# no-random-merge-tree-settings: the surgery injects standalone (non-packed)
# index files, so `packed_skip_index_max_bytes` is pinned to 0 below.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_orphan SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_orphan
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

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_orphan (k, v) SELECT number, number FROM numbers(2000)"

materialized () {
    ${CLICKHOUSE_CLIENT} -q "
    SELECT secondary_indices_materialized
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_orphan' AND active AND rows > 0
    ORDER BY name"
}

echo "after insert:"
materialized

data_path=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_orphan'")
active=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_orphan' AND active AND rows > 0 LIMIT 1")

# Save the freshly written index files, then drop and re-add the index so the
# active part has no `skp_idx_mm_v` entries in `checksums.txt` any more.
cp "${active}skp_idx_mm_v.idx2" "${data_path}/saved.idx2"
cp "${active}skp_idx_mm_v.cmrk2" "${data_path}/saved.cmrk2"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_orphan DROP INDEX mm_v SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_orphan ADD INDEX mm_v v TYPE minmax GRANULARITY 1"

echo "after drop and re-add:"
materialized

# Re-inject the saved files as orphans: present on disk, absent from checksums.
corrupt=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_orphan' AND active AND rows > 0 LIMIT 1")
cp "${data_path}/saved.idx2" "${corrupt}skp_idx_mm_v.idx2"
cp "${data_path}/saved.cmrk2" "${corrupt}skp_idx_mm_v.cmrk2"

# The orphan file is on disk, but the index is still not materialized here.
echo "orphan file on disk:"
if ls "${corrupt}"skp_idx_mm_v.* >/dev/null 2>&1; then echo 1; else echo 0; fi
echo "with orphan file:"
materialized

# The first `MATERIALIZE INDEX` only drops the orphan files (04427): the index is
# not on disk any more, so it is not selected for recalculation. The second one
# rebuilds it, and only then is it materialized -- which also shows the check is
# not vacuously false.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_orphan MATERIALIZE INDEX mm_v SETTINGS mutations_sync = 2"
echo "after first materialize index:"
materialized
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_orphan MATERIALIZE INDEX mm_v SETTINGS mutations_sync = 2"
echo "after second materialize index:"
materialized

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_orphan SYNC"

# A packed index has no per-file entry in `checksums.txt` of its own -- its data is
# a member of the part's `skp_idx.packed`, which is what is checksummed. It is
# materialized all the same, so the check must not read the missing entry as absence.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_packed SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_packed
(
    k UInt64,
    v UInt64,
    INDEX mm_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         packed_skip_index_max_bytes = 1048576,
         columns_and_secondary_indices_sizes_lazy_calculation = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_packed (k, v) SELECT number, number FROM numbers(2000)"

packed_part=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_packed' AND active AND rows > 0 LIMIT 1")
echo "packed archive written:"
if [ -f "${packed_part}skp_idx.packed" ]; then echo 1; else echo 0; fi
echo "packed index materialized:"
${CLICKHOUSE_CLIENT} -q "
SELECT secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_packed' AND active AND rows > 0
ORDER BY name"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_packed SYNC"
