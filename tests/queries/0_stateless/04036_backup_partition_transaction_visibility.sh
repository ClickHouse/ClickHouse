#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage

# Regression test for PR #99775:
# getVisibleDataPartsVectorInPartitions used Active state twice instead of
# Active + Outdated when inside a transaction, causing BACKUP PARTITION to
# fail with BACKUP_ENTRY_ALREADY_EXISTS.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh
. "$CURDIR"/transactions.lib

TABLE="${CLICKHOUSE_DATABASE}.test_backup_txn_partition"
BACKUP_NAME="${CLICKHOUSE_DATABASE}_backup_txn_$$"

# old_parts_lifetime=3600 prevents Outdated parts from being cleaned up during the test.
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} (id UInt64)
ENGINE = MergeTree()
PARTITION BY id % 2
ORDER BY id
SETTINGS old_parts_lifetime=3600;"

# Insert rows into partition 0.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} VALUES (0), (2)"

# Rewrite the part in partition 0 BEFORE the transaction starts (OPTIMIZE FINAL rewrites
# even a single part). After this, partition 0 has one Active part and one Outdated original.
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ${TABLE} PARTITION 0 FINAL"

# Begin transaction — snapshot T1 is captured after the merge is committed.
tx 1 "BEGIN TRANSACTION" | grep -v '^$' ||:
tx 1 "SET throw_on_unsupported_query_inside_transaction=0" | grep -v '^$' ||:

# BACKUP inside the transaction using snapshot T1.
# With the bug (Active collected twice):
#   - Merged part added to backup twice -> BACKUP_ENTRY_ALREADY_EXISTS error.
# With the fix (Active + Outdated collected):
#   - Rewritten part collected once (Active); Outdated original filtered out
#     (removal_csn <= T1) -> backup succeeds with 2 rows.
tx 1 "BACKUP TABLE ${TABLE} PARTITION 0 TO Memory('${BACKUP_NAME}') FORMAT Null" | grep -v '^$' ||:
tx 1 "COMMIT" | grep -v '^$' ||:

# Restore must use the same HTTP session as BACKUP since Memory backups are session-scoped.
tx 1 "DROP TABLE IF EXISTS ${TABLE}_restored" | grep -v '^$' ||:
tx 1 "RESTORE TABLE ${TABLE} AS ${TABLE}_restored FROM Memory('${BACKUP_NAME}') FORMAT Null" | grep -v '^$' ||:

tx 1 "SELECT count() FROM ${TABLE}_restored" | cut -f2-

# Cleanup
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS ${TABLE};
DROP TABLE IF EXISTS ${TABLE}_restored;"
