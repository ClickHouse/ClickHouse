#!/usr/bin/env bash
# Tags: no-fasttest


CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_traversal"
SECRET_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_secret.txt"

rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_DIR}/_delta_log"
echo "TOP_SECRET_CONTENTS" > "${SECRET_FILE}"

SECRET_REL="$(realpath --relative-to="${TABLE_DIR}" "${SECRET_FILE}")"

cat > "${TABLE_DIR}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"exploit","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"line\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
{"add":{"path":"${SECRET_REL}","size":100000,"modificationTime":1700000000000,"dataChange":true,"partitionValues":{}}}
EOF

# Run the malicious log against both Delta readers: the delta-kernel-rs reader
# (allow_experimental_delta_kernel_rs = 1) and the legacy DeltaLakeMetadata
# reader (allow_experimental_delta_kernel_rs = 0). The path-containment
# invariant must hold for both.
check_reader() {
    local kernel="$1"
    echo "--- allow_experimental_delta_kernel_rs = ${kernel} ---"

    ${CLICKHOUSE_LOCAL} --allow_experimental_delta_kernel_rs="${kernel}" -q \
        "SELECT * FROM deltaLakeLocal('${TABLE_DIR}', 'RawBLOB') LIMIT 100 FORMAT TabSeparated" 2>&1 \
        | grep -q 'PATH_ACCESS_DENIED' && echo "GOT ACCESS DENIED ERROR"

    ${CLICKHOUSE_LOCAL} --allow_experimental_delta_kernel_rs="${kernel}" -q \
        "SELECT * FROM deltaLakeLocal('${TABLE_DIR}', 'RawBLOB') LIMIT 100 FORMAT TabSeparated" 2>&1 \
        | grep -q 'TOP_SECRET_CONTENTS' && echo "LEAKED" || echo "NO LEAK"
}

check_reader 1
check_reader 0

rm -rf "${TABLE_DIR}" "${SECRET_FILE}"
