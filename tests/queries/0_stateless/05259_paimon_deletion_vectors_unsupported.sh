#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no Paimon support, and the engine arms read the S3 copy of data_minio

# Reading a Paimon snapshot whose index manifest lists deletion vectors must throw in every read mode (full
# scan, targeted snapshot, incremental), because the deletion vectors are not applied, and a refused
# incremental read must not advance its Keeper watermark. This holds after 'deletion-vectors.enabled' is
# turned off, while snapshots without deletion vectors keep reading.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Per-run directory: the flaky check runs this file concurrently with itself.
DATA_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/data_minio"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
cp -r "${CUR_DIR}/data_minio/paimon_append_deletion_vectors/" "${DATA_DIR}/"
cp -r "${CUR_DIR}/data_minio/paimon_append_deletion_vectors_disabled/" "${DATA_DIR}/"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition/" "${DATA_DIR}/"

DV_TABLE="${DATA_DIR}/paimon_append_deletion_vectors"

# An UPDATE marked the old version of id = 1 as deleted; returning it would be a wrong result.
out=$(${CLICKHOUSE_CLIENT} -q "SELECT * FROM paimonLocal('${DV_TABLE}') ORDER BY id, val;" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "SELECT THROWS"

out=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${DV_TABLE}');" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "COUNT THROWS"

# The same table after 'deletion-vectors.enabled' was turned off and one more row was inserted: the latest
# snapshot's schema says 'false', but its index manifest still lists the deletion vector.
out=$(${CLICKHOUSE_CLIENT} -q "SELECT * FROM paimonLocal('${DATA_DIR}/paimon_append_deletion_vectors_disabled') ORDER BY id, val;" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "DISABLED THROWS"

${CLICKHOUSE_CLIENT} -q "DESCRIBE paimonLocal('${DV_TABLE}');"

# The targeted and incremental read modes are only reachable through the table engine.
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine 1 -q "
    DROP TABLE IF EXISTS t_dv;
    CREATE TABLE t_dv ENGINE = PaimonS3(s3_conn, filename = 'paimon_append_deletion_vectors')
    SETTINGS
        paimon_incremental_read = 1,
        paimon_keeper_path = '/clickhouse/tables/{database}/t_dv',
        paimon_replica_name = '{replica}';"

# Snapshot 3 is the one that added the deletion vector.
out=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dv SETTINGS paimon_target_snapshot_id = 3;" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "TARGETED THROWS"

# Snapshot 2 predates it, so it is read.
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_dv ORDER BY id, val SETTINGS paimon_target_snapshot_id = 2;"

out=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dv;" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "INCREMENTAL THROWS"

# The refused incremental read must not record a consumed snapshot.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/${CLICKHOUSE_DATABASE}/t_dv' AND name = 'committed_snapshot';"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dv;"

# A reader that already consumed snapshot 2 reaches snapshot 3, the compaction that added the deletion vector.
# The writer's commit of snapshot 3 is replayed by copying its snapshot file and the LATEST hint.
RESUMED_TABLE="${DATA_DIR}/dv_resumed"
cp -r "${DV_TABLE}" "${RESUMED_TABLE}"
rm "${RESUMED_TABLE}/snapshot/snapshot-3"
printf '2' > "${RESUMED_TABLE}/snapshot/LATEST"

${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine 1 -q "
    DROP TABLE IF EXISTS t_dv_resumed;
    CREATE TABLE t_dv_resumed ENGINE = PaimonLocal('${RESUMED_TABLE}')
    SETTINGS
        paimon_incremental_read = 1,
        paimon_keeper_path = '/clickhouse/tables/{database}/t_dv_resumed',
        paimon_replica_name = '{replica}';"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dv_resumed;"

cp "${DV_TABLE}/snapshot/snapshot-3" "${DV_TABLE}/snapshot/LATEST" "${RESUMED_TABLE}/snapshot/"

out=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dv_resumed;" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "RESUMED THROWS"

# The watermark stays at the consumed snapshot 2.
${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/${CLICKHOUSE_DATABASE}/t_dv_resumed' AND name = 'committed_snapshot';"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dv_resumed;"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${DATA_DIR}/paimon_no_partition');"

rm -rf "${DATA_DIR}"
