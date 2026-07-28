#!/usr/bin/env bash
# Tags: zookeeper, no-object-storage, no-encrypted-storage, no-shared-merge-tree, no-replicated-database
# zookeeper: transactions store version metadata via the transaction log in ZooKeeper.
# no-object-storage / no-encrypted-storage: the test writes a raw `txn_version.txt.tmp` next to
#   `data.packed` on the local filesystem; on object storage or encrypted disks files carry extra
#   metadata, so a raw write does not reproduce the on-disk state under test.
# no-shared-merge-tree: uses plain MergeTree with a locally tampered part.
# no-replicated-database: the test tampers with the on-disk part between operations.
#
# Regression test for packed part storage losing the stale `txn_version.txt.tmp` guard.
#
# When a transaction stores version metadata, `VersionMetadataOnDisk::storeInfoToDataPartStorage`
# calls `createFile` for `txn_version.txt.tmp` first, relying on it to throw if a stale tmp file is
# already there (a leftover from a crashed write). For full part storage `createFile` maps to
# `DiskLocal::createFile` and throws `CANNOT_CREATE_FILE`. For packed storage `createFile` used to be
# a silent no-op, so the stale tmp was overwritten and the guard was lost. This test forces packed
# storage, plants a stale tmp file, then triggers a version-metadata store via part removal: the
# removal must be rejected by the guard, exactly as it is for full storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Transactions reject async inserts, and clickhouse-test may randomize async_insert on.
CH_CLIENT="${CLICKHOUSE_CLIENT} --async_insert=0"

${CH_CLIENT} -q "DROP TABLE IF EXISTS packed_stale_tmp SYNC"

# min_bytes_for_full_part_storage forces packed storage (the whole part in a single data.packed).
${CH_CLIENT} -q "
CREATE TABLE packed_stale_tmp (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_full_part_storage = '1G', min_bytes_for_wide_part = 0, old_parts_lifetime = 100000;"

# Create the part inside a transaction so txn_version.txt is persisted on disk (not deferred).
${CH_CLIENT} -q "BEGIN TRANSACTION; INSERT INTO packed_stale_tmp VALUES (1); COMMIT;"

# Sanity check: the part must actually be packed for this test to be meaningful.
${CH_CLIENT} -q "SELECT part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 'packed_stale_tmp' AND active"

PART_PATH=$(${CH_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'packed_stale_tmp' AND active")

# Plant a stale temporary version file, as a crashed metadata write would leave behind.
echo "incomplete" > "${PART_PATH}txn_version.txt.tmp"

# Removing the part stores a removal TID, which goes through the createFile stale-tmp guard. With the
# stale tmp present the removal must be rejected (as it is for full storage), not silently accepted.
DROP_RESULT=$(${CH_CLIENT} -q "ALTER TABLE packed_stale_tmp DROP PART 'all_1_1_0'" 2>&1)
if echo "${DROP_RESULT}" | grep -qF "File exists"; then
    echo "drop rejected by stale tmp guard"
else
    echo "drop NOT rejected (bug): ${DROP_RESULT}"
fi

# The rejected removal must leave the part and its row intact.
echo "active parts: $(${CH_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'packed_stale_tmp' AND active")"
echo "rows: $(${CH_CLIENT} -q "SELECT count() FROM packed_stale_tmp")"

${CH_CLIENT} -q "DROP TABLE packed_stale_tmp SYNC"
