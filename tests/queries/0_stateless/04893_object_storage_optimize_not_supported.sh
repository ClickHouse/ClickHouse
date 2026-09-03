#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-replicated-database
# Tag no-fasttest: delta-kernel and Paimon pull in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.
# Tag no-replicated-database: kept for PaimonLocal, which no other test exercises without it.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114765
# OPTIMIZE TABLE on an object-storage table whose configuration does not implement
# compaction used to return Ok and do nothing. The two unsupported defaults
# (StorageObjectStorageConfiguration::optimize for plain object storage,
# IDataLakeMetadata::optimize for lakes) returned false, and the interpreter discards
# that bool, so the statement reported success. They now throw, like every other
# unsupported operation default in the same two headers.
#
# Iceberg overrides `IDataLakeMetadata::optimize`, so it is unaffected and not covered here:
# its own gate message is worded differently in the Cloud build.
#
# The empty Delta table is bootstrapped by hand (a v0 _delta_log with only protocol +
# metaData), because ClickHouse cannot initialize a Delta transaction log itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_optimize_unsupported"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"
mkdir -p "${ROOT}/delta/_delta_log"

cat > "${ROOT}/delta/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-optimize","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"k\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF

cp -r "${CUR_DIR}/data_minio/paimon_no_partition" "${ROOT}/paimon"

# DeltaLake, through both metadata readers: allow_delta_kernel_rs picks
# DeltaLakeMetadataDeltaKernel (1) or DeltaLakeMetadata (0), and both inherit the default.
for kernel in 1 0; do
    echo "-- delta, allow_delta_kernel_rs = ${kernel}"
    ${CLICKHOUSE_CLIENT} --allow_delta_kernel_rs="${kernel}" --query "
        DROP TABLE IF EXISTS t_delta;
        CREATE TABLE t_delta ENGINE = DeltaLakeLocal('${ROOT}/delta');
        OPTIMIZE TABLE t_delta;
    " 2>&1 | grep -o 'Method `optimize` is not implemented for DeltaLake' | head -n 1
done

echo "-- paimon"
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine=1 --query "
    DROP TABLE IF EXISTS t_paimon;
    CREATE TABLE t_paimon ENGINE = PaimonLocal('${ROOT}/paimon');
    OPTIMIZE TABLE t_paimon;
" 2>&1 | grep -o 'Method `optimize` is not implemented for Paimon' | head -n 1

# Plain object storage reaches StorageObjectStorageConfiguration::optimize instead, which
# reports the engine type. The statement is refused before any request is issued, so this
# needs no reachable endpoint.
echo "-- plain object storage (s3)"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_s3;
    CREATE TABLE t_s3 (k UInt64) ENGINE = S3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_optimize.parquet', 'clickhouse', 'clickhouse', Parquet);
    OPTIMIZE TABLE t_s3;
" 2>&1 | grep -o "Table engine s3 doesn't support optimize" | head -n 1

# Control: a supported engine still optimizes, and a genuine no-op is still not an error
# unless optimize_throw_if_noop asks for one.
echo "-- mergetree control"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_mt;
    CREATE TABLE t_mt (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_mt SELECT number FROM numbers(5);
    OPTIMIZE TABLE t_mt;
    SELECT 'optimized';
"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_mt SETTINGS optimize_throw_if_noop = 1" 2>&1 \
    | grep -o 'CANNOT_ASSIGN_OPTIMIZE' | head -n 1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_delta; DROP TABLE IF EXISTS t_paimon; DROP TABLE IF EXISTS t_s3; DROP TABLE IF EXISTS t_mt"
