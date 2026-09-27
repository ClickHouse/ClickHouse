#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no Paimon support, and the engine arms read the S3 copy of data_minio

# Reading a Paimon table with deletion vectors enabled must throw in every read mode (full scan,
# targeted snapshot, incremental), because the deletion vectors are not applied. The table stays
# inspectable, and tables without deletion vectors keep reading.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Per-run directory: the flaky check runs this file concurrently with itself.
DATA_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/data_minio"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
cp -r "${CUR_DIR}/data_minio/paimon_append_deletion_vectors/" "${DATA_DIR}/"
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

${CLICKHOUSE_CLIENT} -q "DESCRIBE paimonLocal('${DV_TABLE}');"

# The targeted and incremental read modes are only reachable through the table engine.
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine 1 -q "
    DROP TABLE IF EXISTS t_dv;
    CREATE TABLE t_dv ENGINE = PaimonS3(s3_conn, filename = 'paimon_append_deletion_vectors')
    SETTINGS
        paimon_incremental_read = 1,
        paimon_keeper_path = '/clickhouse/tables/{database}/t_dv',
        paimon_replica_name = '{replica}';"

out=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dv SETTINGS paimon_target_snapshot_id = 2;" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "TARGETED THROWS"

out=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dv;" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "INCREMENTAL THROWS"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dv;"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${DATA_DIR}/paimon_no_partition');"

rm -rf "${DATA_DIR}"
