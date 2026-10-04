#!/usr/bin/env bash
# Tags: no-fasttest, no-msan


CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_wakeup"
SECRET_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_wakeup_secret.txt"

rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_DIR}/_delta_log"
echo "TOP_SECRET_CONTENTS" > "${SECRET_FILE}"

SECRET_REL="$(realpath --relative-to="${TABLE_DIR}" "${SECRET_FILE}")"

cat > "${TABLE_DIR}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"exploit","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"line\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
{"add":{"path":"${SECRET_REL}","size":100000,"modificationTime":1700000000000,"dataChange":true,"partitionValues":{}}}
EOF

# The failpoint orders the delta-kernel scan thread against the reading pipeline so that the
# scan thread publishes its path-containment error while the pipeline sits between evaluating
# the `data_files_cv` predicate and registering on that condition variable. The error must
# still surface: `timeout` bounds the run because a dropped notification makes the read wait
# with no deadline, which the test harness can only end by killing the process.
OUTPUT=$(timeout 60 ${CLICKHOUSE_LOCAL} --allow_experimental_delta_kernel_rs=1 -q "
    SYSTEM ENABLE FAILPOINT delta_lake_iterator_sleep_in_scan_handoff;
    SELECT * FROM deltaLakeLocal('${TABLE_DIR}', 'RawBLOB') LIMIT 100 FORMAT TabSeparated
" 2>&1)
RESULT=$?

[ "${RESULT}" = "124" ] && echo "TIMED OUT" || echo "NOT TIMED OUT"

# Match the containment check inside the scan callback, not just the error code: the table
# function has an outer "File path ... is not inside ..." guard that raises the same code
# without ever reaching the delta reader.
echo "${OUTPUT}" | grep -q 'should be inside the table directory' \
    && echo "GOT PATH CONTAINMENT ERROR" || echo "NO PATH CONTAINMENT ERROR"

echo "${OUTPUT}" | grep -q 'TOP_SECRET_CONTENTS' && echo "LEAKED" || echo "NO LEAK"

rm -rf "${TABLE_DIR}" "${SECRET_FILE}"
