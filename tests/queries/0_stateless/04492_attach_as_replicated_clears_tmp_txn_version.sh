#!/usr/bin/env bash
# Tags: zookeeper, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-encrypted-storage
# no-object-storage / no-encrypted-storage: the test writes raw text into
#   `txn_version.txt.tmp` on the part's directory on a local filesystem. On object storage or
#   encrypted disks files are wrapped in extra metadata, so a raw write does not reproduce the
#   on-disk state under test.
#
# Regression test for ATTACH TABLE ... AS REPLICATED discarding committed data when a stale
# `txn_version.txt.tmp` is left on a part.
#
# `InterpreterCreateQuery::clearTransactionMetadata` (run by ATTACH AS REPLICATED) used to
# remove only `txn_version.txt`, not `txn_version.txt.tmp`. A `.tmp` file can legitimately
# linger on a part (for example, hardlinked onto a mutated part from its source part during a
# merge/mutation race on object storage). With `txn_version.txt` gone but the `.tmp` present,
# `VersionMetadataOnDisk::loadMetadata` treats the part as a rolled-back transaction
# (`creation_csn == Tx::RolledBackCSN`), so it is marked `Outdated` and discarded. The
# committed data is then lost - here the whole table would wrongly become empty.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ATTACH AS REPLICATED must remove the stale tmp file, so nothing should warn. Keep
# error-level logs only so an unexpected warning would still surface as a failure.
CH_CLIENT="${CLICKHOUSE_CLIENT} --send_logs_level=error"

PART_PATH=$(${CH_CLIENT} -q "
    DROP TABLE IF EXISTS t_attach_repl_tmp_txn;
    CREATE TABLE t_attach_repl_tmp_txn (n Int64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO t_attach_repl_tmp_txn VALUES (42);
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 't_attach_repl_tmp_txn' AND active
    LIMIT 1
")

if [[ -z "${PART_PATH}" ]]; then
    echo "FAIL: could not locate active part for t_attach_repl_tmp_txn"
    exit 1
fi

# Simulate a leftover temporary version file: drop a `txn_version.txt.tmp` into the part
# directory while keeping `txn_version.txt` absent.
echo "incomplete" > "${PART_PATH}/txn_version.txt.tmp"

# ATTACH AS REPLICATED must strip the leftover tmp file together with `txn_version.txt`, so
# the part loads as plain committed data and the row is preserved (count 1, not 0).
${CH_CLIENT} -n -q "
    DETACH TABLE t_attach_repl_tmp_txn SYNC;
    ATTACH TABLE t_attach_repl_tmp_txn AS REPLICATED;

    SELECT 'count_after_attach', count() FROM t_attach_repl_tmp_txn;
    SELECT 'value_after_attach', n FROM t_attach_repl_tmp_txn ORDER BY n;

    DETACH TABLE t_attach_repl_tmp_txn SYNC;
    ATTACH TABLE t_attach_repl_tmp_txn AS NOT REPLICATED;

    DROP TABLE t_attach_repl_tmp_txn;
"
