#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage, no-replicated-database, no-parallel, no-shared-merge-tree, no-object-storage, no-flaky-check
# no-object-storage: the test writes raw text into `txn_version.txt.tmp` on the part's
#   directory to simulate an incomplete atomic write on a local filesystem. On object
#   storage disks every file is wrapped in `DiskObjectStorageMetadata`; reading the bogus
#   file through `VersionMetadataOnDisk::removeTmpMetadataFile` then fails with
#   `CANNOT_PARSE_INPUT_ASSERTION_FAILED` before the scenario under test is reached.
# no-flaky-check: the test is deterministic by design (it constructs the specific on-disk
#   state and asserts the `DETACH`+`ATTACH` behavior). Running it 50x in parallel inside
#   the flaky-check / targeted variants under sanitizers exceeds the 600s per-test budget.
# Regression test for https://github.com/ClickHouse/ClickHouse/pull/92141
# (STID 3547-447e):
#
# When a part on disk has `txn_version.txt.tmp` but no `txn_version.txt` (an
# incomplete write that was not atomically renamed), `VersionMetadataOnDisk::loadMetadata`
# returns a `VersionInfo` with `creation_tid = Tx::DummyTID` and
# `creation_csn = Tx::RolledBackCSN`. `DummyTID` has `start_csn == NonTransactionalCSN`
# but `local_tid == DummyLocalTID`, which must not trip the assertion inside
# `TransactionID::isNonTransactional` called by `VersionMetadata::validateInfo` and
# `VersionInfo::wasInvolvedInTransaction`. Before the fix, the server aborted
# with signal 6 during part loading in debug and sanitizer builds.
#
# This test creates a part with no `txn_version.txt` (deferred persist is the
# default for non-transactional inserts), drops a bogus `txn_version.txt.tmp`
# into the part directory, and runs `DETACH` + `ATTACH`. The server must stay
# alive and the rolled-back part must be invisible.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The `ATTACH TABLE` below triggers `VersionMetadataOnDisk::removeTmpMetadataFile`, which
# emits a `LOG_WARNING` about the bogus `txn_version.txt.tmp` left on disk. This is
# exactly the code path this regression test exercises — suppress the expected warning
# so the Fast test runner does not flag the stderr as a failure.
CH_CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level=error"

# Set up the table, insert one row, and return the active part path. Combined into a
# single client invocation so the `flaky check` job (runs the test 50x in parallel under
# all sanitizers) stays within the shared 45-minute budget — each extra client round-trip
# costs ~1 second under MSAN/TSAN.
PART_PATH=$(${CH_CLIENT} -q "
    DROP TABLE IF EXISTS t_txn_tmp_leftover;
    CREATE TABLE t_txn_tmp_leftover (n Int64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO t_txn_tmp_leftover VALUES (42);
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 't_txn_tmp_leftover' AND active
    LIMIT 1
")

if [[ -z "${PART_PATH}" ]]; then
    echo "FAIL: could not locate active part for t_txn_tmp_leftover"
    exit 1
fi

# Simulate an incomplete write: drop a bogus `txn_version.txt.tmp` file into the
# part directory while keeping `txn_version.txt` absent.
echo "incomplete" > "${PART_PATH}/txn_version.txt.tmp"

# DETACH + ATTACH triggers `VersionMetadataOnDisk::loadMetadata` on the stale tmp
# file. Before the fix this aborted the server with signal 6 (assertion inside
# `TransactionID::isNonTransactional`). With the fix the part is loaded with
# `creation_tid == Tx::DummyTID` / `creation_csn == Tx::RolledBackCSN`, marked
# `Outdated` by `MergeTreeData::loadDataPart`, and safely cleaned up on `DROP`.
#
# Assertions:
#   - `SELECT count()` is 0 because the rolled-back part is inactive.
#   - The rolled-back part is present in `system.parts` with `active = 0`,
#     `is_dummy_tid = 1`, `is_rolled_back_csn = 1`.
#   - `DROP TABLE` completes — the fix in `VersionMetadata::hasValidMetadata`
#     lets the part be removed without trying to read the missing on-disk metadata.
${CH_CLIENT} -q "
    DETACH TABLE t_txn_tmp_leftover;
    ATTACH TABLE t_txn_tmp_leftover;

    SELECT 'select_ok', count() FROM t_txn_tmp_leftover;

    SELECT
        'rolled_back_part',
        active,
        creation_tid = (1, 2, '00000000-0000-0000-0000-000000000000') AS is_dummy_tid,
        creation_csn = 18446744073709551615                               AS is_rolled_back_csn
    FROM system.parts
    WHERE database = currentDatabase()
        AND table = 't_txn_tmp_leftover'
        AND creation_csn = 18446744073709551615;

    DROP TABLE t_txn_tmp_leftover;
"
