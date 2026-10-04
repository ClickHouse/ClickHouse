#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 'http_max_multipart_form_data_size' is a budget shared by all external-table parts of a request.
# A native-compressed ('_decompress') part must be limited by what is left of the budget after the
# previous parts rather than by the whole limit, otherwise several small compressed parts could
# materialize more external data than the limit allows.

USER_NAME="test_compressed_parts_budget_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = 1500000"

DATA_SMALL="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_small.native"
DATA_LARGE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_large.native"

# Constant values compress to a few kilobytes, so the compressed payloads are far below the limit.
# 50,000 UInt64 values are about 400 KB decompressed, 100,000 values are about 800 KB.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+CAST(0,'UInt64')+AS+id+FROM+numbers(50000)+FORMAT+Native&compress=1" > "${DATA_SMALL}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+CAST(0,'UInt64')+AS+id+FROM+numbers(100000)+FORMAT+Native&compress=1" > "${DATA_LARGE}"

URL="${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+(SELECT+count()+FROM+ext1)+%2B+(SELECT+count()+FROM+ext2)&ext1_structure=id+UInt64&ext1_format=Native&ext1_decompress=1&ext2_structure=id+UInt64&ext2_format=Native&ext2_decompress=1"

# Two parts of about 400 KB each fit the 1.5 MB budget together.
${CLICKHOUSE_CURL} -sS -F "ext1=@${DATA_SMALL}" -F "ext2=@${DATA_SMALL}" "${URL}"

# Two parts of about 800 KB each fit the limit one by one, but not together.
${CLICKHOUSE_CURL} -sS -F "ext1=@${DATA_LARGE}" -F "ext2=@${DATA_LARGE}" "${URL}" 2>&1 | grep -o 'TOO_LARGE_SIZE_COMPRESSED' | head -n1

rm -f "${DATA_SMALL}" "${DATA_LARGE}"

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
