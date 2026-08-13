#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# no-fasttest: vector_spann requires the USearch contrib, which is not built in the fast test.
# no-object-storage / no-shared-merge-tree / no-replicated-database: the test inspects the part
#   directory on local disk; on object storage the files there are metadata pointers, not real data.
# no-random-merge-tree-settings: the wide-part path (min_bytes_for_wide_part = 0) must be deterministic.
#
# Regression for DROP INDEX on a wide part with a vector_spann index. vector_spann stores a second
# substream (skp_idx_<name>.pl.idx, the posting lists) besides the regular skp_idx_<name>.idx.
# collectFilesForRenames() used to reconstruct the dropped index from the mutation's metadata
# snapshot, but ALTER ... DROP INDEX removes the index from metadata before the background mutation
# runs, so the lookup returned null and the removal was silently skipped. On wide parts the partial
# mutation then hardlinked the stale skp_idx_<name>.* files (including the .pl posting lists) into the
# new part. Assert that no skp_idx_* files survive in the active part directory after the drop.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
SET allow_experimental_vector_spann_index = 1;
DROP TABLE IF EXISTS tab_spann_drop_files;
CREATE TABLE tab_spann_drop_files
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_spann('spann', 'L2Distance', 2, 'bf16', 32, 128, 1.0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4;

INSERT INTO tab_spann_drop_files SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(16);
"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 'tab_spann_drop_files'")

part_index_files() {
    local part
    part=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'tab_spann_drop_files' AND active LIMIT 1")
    ls "${DATA_PATH}/${part}/" 2>/dev/null | grep -c '^skp_idx_'
}

before=$(part_index_files)
echo "skp_idx files before drop > 0: $([ "${before}" -gt 0 ] && echo 1 || echo 0)"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE tab_spann_drop_files DROP INDEX idx SETTINGS mutations_sync = 1;"

echo "skp_idx files after drop: $(part_index_files)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE tab_spann_drop_files;"
