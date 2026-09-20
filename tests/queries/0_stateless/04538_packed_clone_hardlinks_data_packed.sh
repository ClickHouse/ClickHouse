#!/usr/bin/env bash
# Tags: no-object-storage, no-shared-merge-tree
# no-object-storage: compares on-disk data.packed inodes, i.e. a local-filesystem hardlink check.
# no-shared-merge-tree: uses plain MergeTree on local storage.
#
# A clone of an unchanged packed part (e.g. an untouched-part mutation) must hardlink data.packed
# into the new part, not byte-copy it. The freeze fast path used by cloneAndLoadDataPart copied the
# whole archive instead of hardlinking, a large regression for the local no-op-mutation / clone path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS packed_clone_hardlink SYNC"

# min_bytes_for_full_part_storage forces packed storage; keep the source part around after the
# mutation so both archives can be compared; do not force copies over hardlinks.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE packed_clone_hardlink (a UInt64, s String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_full_part_storage = '1G', min_bytes_for_wide_part = 0,
         always_use_copy_instead_of_hardlinks = 0, old_parts_lifetime = 100000;
INSERT INTO packed_clone_hardlink SELECT number, toString(number) FROM numbers(1000);"

# Sanity check: the part must actually be packed for this test to be meaningful.
${CLICKHOUSE_CLIENT} -q "SELECT part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 'packed_clone_hardlink' AND active"

SRC_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'packed_clone_hardlink' AND active")

# A mutation that touches nothing clones the part unchanged instead of rewriting it.
${CLICKHOUSE_CLIENT} --mutations_sync=1 -q "ALTER TABLE packed_clone_hardlink DELETE WHERE 0"

DST_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'packed_clone_hardlink' AND active")

# The unchanged data.packed must be shared via a hardlink (same inode), not byte-copied.
SRC_INODE=$(stat -c %i "${SRC_PATH}data.packed")
DST_INODE=$(stat -c %i "${DST_PATH}data.packed")
if [ "${SRC_INODE}" = "${DST_INODE}" ]; then
    echo "data.packed hardlinked"
else
    echo "data.packed copied"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE packed_clone_hardlink SYNC"
