#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow"
PROFILE_EVENTS_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.profile_events"
trap 'rm -f "${DATA_FILE}" "${PROFILE_EVENTS_FILE}"' EXIT

OPENSSL_CONF=/dev/null python3 - <<EOF
import pyarrow as pa
import pyarrow.ipc as ipc

rows = 1000
batch = pa.record_batch(
    [
        pa.array(range(rows), type=pa.uint64()),
        pa.array(["x" * 1000] * rows, type=pa.string()),
    ],
    names=["small", "large"],
)

with ipc.new_file("${DATA_FILE}", batch.schema) as writer:
    writer.write_batch(batch)
EOF

get_selected_bytes()
{
    ${CLICKHOUSE_LOCAL} --print-profile-events --query "$1" > /dev/null 2> "${PROFILE_EVENTS_FILE}"
    sed -n 's/.*SelectedBytes: \([0-9][0-9]*\).*/\1/p' "${PROFILE_EVENTS_FILE}" | tail -1
}

projected_bytes=$(get_selected_bytes "SELECT small FROM file('${DATA_FILE}', 'Arrow', 'small UInt64, large String') FORMAT Null")
all_bytes=$(get_selected_bytes "SELECT small, large FROM file('${DATA_FILE}', 'Arrow', 'small UInt64, large String') FORMAT Null")

if (( projected_bytes * 4 >= all_bytes ))
then
    echo "Expected projected Arrow read to be much smaller than full read, got projected=${projected_bytes}, all=${all_bytes}"
    exit 1
fi

echo 'Ok'
