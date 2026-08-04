#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: cas is an object-storage metadata type; not available on the minimal
#              fasttest image.

# CA BACKUP/RESTORE round-trip oracle: proves a table on a content-addressed (CA) disk survives a
# full BACKUP -> DROP -> RESTORE cycle with byte-for-byte data equality, including a PROJECTION.
# RESTORE materializes each part through one whole-part ContentAddressedTransaction
# (restorePartFromBackup, commit d384298602b); BACKUP-read already worked. This is the inline-CA
# oracle for B16/B34.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cas_br;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cas_br_restored;"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_cas_br (k UInt32, v String, PROJECTION p (SELECT k, count() GROUP BY k))
ENGINE = MergeTree ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05005',
    name = '05005_cas_backup_restore',
    path = '05005_cas_backup_restore_pool/');"

# Two inserts -> two parts; deterministic rows.
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cas_br VALUES (1, 'a'), (2, 'b'), (1, 'c');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cas_br VALUES (3, 'd'), (2, 'e');"

${CLICKHOUSE_CLIENT} --query "SELECT 'source', count(), sum(k), arraySort(groupArray(v)) FROM t_cas_br;"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t_cas_br TO ${backup_name} FORMAT Null;"

${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t_cas_br AS t_cas_br_restored FROM ${backup_name} FORMAT Null;"

# Round-trip data equality on the restored table.
${CLICKHOUSE_CLIENT} --query "SELECT 'restored', count(), sum(k), arraySort(groupArray(v)) FROM t_cas_br_restored;"

# Projection-served query on the restored table (proves the projection round-tripped).
${CLICKHOUSE_CLIENT} --query "SELECT 'projection', k, count() FROM t_cas_br_restored GROUP BY k ORDER BY k SETTINGS force_optimize_projection = 1;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cas_br;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cas_br_restored;"
${CLICKHOUSE_CLIENT} --query "SELECT 'done';"
