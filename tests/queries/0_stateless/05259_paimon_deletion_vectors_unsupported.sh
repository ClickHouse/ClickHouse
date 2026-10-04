#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no Paimon support, and the engine arms read the S3 copy of data_minio

# Reading a Paimon table with deletion vectors enabled must throw in every read mode (full scan,
# targeted snapshot, incremental), because the deletion vectors are not applied, and a refused
# incremental read must not advance its Keeper watermark. The option is matched case-insensitively,
# the table stays inspectable, and tables without the option or with it set to 'false' keep reading.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Per-run directory: the flaky check runs this file concurrently with itself.
DATA_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/data_minio"
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"
cp -r "${CUR_DIR}/data_minio/paimon_append_deletion_vectors/" "${DATA_DIR}/"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition/" "${DATA_DIR}/"
# The same tables with the option spelled 'TRUE' and set explicitly to 'false'.
cp -r "${CUR_DIR}/data_minio/paimon_append_deletion_vectors" "${DATA_DIR}/dv_upper"
sed -i 's/"deletion-vectors.enabled" : "true"/"deletion-vectors.enabled" : "TRUE"/' "${DATA_DIR}/dv_upper/schema/schema-0"
grep -q '"deletion-vectors.enabled" : "TRUE"' "${DATA_DIR}/dv_upper/schema/schema-0" || echo "dv_upper edit failed"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition" "${DATA_DIR}/no_dv_false"
sed -i 's/"options" : { }/"options" : { "deletion-vectors.enabled" : "false" }/' "${DATA_DIR}/no_dv_false/schema/schema-0"
grep -q '"deletion-vectors.enabled" : "false"' "${DATA_DIR}/no_dv_false/schema/schema-0" || echo "no_dv_false edit failed"

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

# The option is matched case-insensitively, as Paimon parses booleans.
out=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${DATA_DIR}/dv_upper');" 2>&1)
echo "$out" | grep -q 'deletion vectors are not applied' \
    && echo "$out" | grep -q 'NOT_IMPLEMENTED' \
    && echo "UPPERCASE THROWS"

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

# The refused incremental read must not record a consumed snapshot.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/${CLICKHOUSE_DATABASE}/t_dv' AND name = 'committed_snapshot';"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dv;"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${DATA_DIR}/paimon_no_partition');"
# An explicit 'false' keeps the table readable.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${DATA_DIR}/no_dv_false');"

rm -rf "${DATA_DIR}"
