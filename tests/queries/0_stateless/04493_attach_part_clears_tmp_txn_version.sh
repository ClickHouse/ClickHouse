#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage, no-encrypted-storage, no-replicated-database
# no-object-storage / no-encrypted-storage: the test writes raw text into
#   `txn_version.txt.tmp` in the part's directory on a local filesystem. On object storage or
#   encrypted disks files are wrapped in extra metadata, so a raw write does not reproduce the
#   on-disk state under test.
# no-replicated-database: the test tampers with the on-disk part between DETACH PART and
#   ATTACH PART, which is not compatible with replicated DDL.
#
# Regression test for ALTER TABLE ... ATTACH PART discarding committed data when a stale
# `txn_version.txt.tmp` is left on the attached part.
#
# ATTACH PART strips transaction metadata before reloading the part
# (`MergeTreeData::loadPartAndFixMetadataImpl` -> `IMergeTreeDataPart::removeVersionMetadata`). It
# used to remove only `txn_version.txt`, not `txn_version.txt.tmp`. A `.tmp` file can legitimately
# linger on a part (for example, hardlinked onto a mutated part from its source part during a
# merge/mutation race on object storage). With the main file stripped but the `.tmp` left behind,
# the next full table load (`MergeTreeData::loadDataPart` -> `VersionMetadataOnDisk::loadMetadata`)
# treats the part as a rolled-back transaction (`creation_csn == Tx::RolledBackCSN`), marks it
# `Outdated` and discards it, so the committed row would be lost.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ATTACH PART must remove the stale tmp file, so nothing should warn. Keep error-level logs only
# so an unexpected warning would still surface as a failure.
CH_CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level=error"

${CH_CLIENT} -q "
    DROP TABLE IF EXISTS t_attach_part_tmp_txn;
    CREATE TABLE t_attach_part_tmp_txn (n Int64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO t_attach_part_tmp_txn VALUES (42);
"

PART_NAME=$(${CH_CLIENT} -q "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_attach_part_tmp_txn' AND active
    LIMIT 1
")

if [[ -z "${PART_NAME}" ]]; then
    echo "FAIL: could not locate active part for t_attach_part_tmp_txn"
    exit 1
fi

# Detach the part so its on-disk directory can be tampered with.
${CH_CLIENT} -q "ALTER TABLE t_attach_part_tmp_txn DETACH PART '${PART_NAME}'"

# Read the detached directory name and path directly, without assuming the exact naming.
read -r DETACHED_NAME DETACHED_PATH < <(${CH_CLIENT} -q "
    SELECT name, path FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 't_attach_part_tmp_txn'
    LIMIT 1 FORMAT TSV
")

if [[ -z "${DETACHED_NAME}" || -z "${DETACHED_PATH}" ]]; then
    echo "FAIL: could not locate detached part for t_attach_part_tmp_txn"
    exit 1
fi

# Simulate a leftover temporary version file on the detached part.
echo "incomplete" > "${DETACHED_PATH}/txn_version.txt.tmp"

# ATTACH PART must strip the leftover tmp file, so a later full reload of the table keeps the part
# active and the committed row survives (count 1, not 0).
${CH_CLIENT} -q "ALTER TABLE t_attach_part_tmp_txn ATTACH PART '${DETACHED_NAME}'"

# Force a full reload from disk, which is where a leftover tmp file would resurrect as a
# rolled-back transaction and discard the part.
${CH_CLIENT} -q "DETACH TABLE t_attach_part_tmp_txn SYNC"
${CH_CLIENT} -q "ATTACH TABLE t_attach_part_tmp_txn"

${CH_CLIENT} -q "
    SELECT 'count_after_attach', count() FROM t_attach_part_tmp_txn;
    SELECT 'value_after_attach', n FROM t_attach_part_tmp_txn ORDER BY n;
    DROP TABLE t_attach_part_tmp_txn;
"
