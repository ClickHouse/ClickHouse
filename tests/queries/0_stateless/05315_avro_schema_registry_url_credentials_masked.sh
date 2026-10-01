#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The password in `format_avro_schema_registry_url` must not appear in the schema registry trace log or
# in the error for an unsupported scheme. Only the masked form scheme://[HIDDEN]@host may be shown.
# The data is the magic byte 0 and schema id 1, so the registry is contacted; port 1 refuses at once.

query() {
    echo "SELECT * FROM format(AvroConfluent, 'a UInt8', '\x00\x00\x00\x00\x01') SETTINGS format_avro_schema_registry_url = '$1', format_avro_schema_registry_max_retries = 0"
}

# Asserts the URL was present but masked: the secret is absent and the masked shape is present, so a
# text that never carried the URL fails instead of passing vacuously.
assert_shape() {
    local label="$1" secret="$2" shape="$3" text
    text=$(cat)
    if echo "$text" | grep -qF "$secret"; then
        echo "$label: FAIL cleartext"
    elif echo "$text" | grep -qF "$shape"; then
        echo "$label: OK masked"
    else
        echo "$label: FAIL url absent"
    fi
}

# Counts the lines carrying the secret, without the client's echo of the submitted query, which
# legitimately contains what the user typed.
count_secret_in_server_output() {
    grep -vF '(query: ' | grep -cF "$1" || true
}

# 1. The trace log line of the schema fetch.
SECRET_A="canaryTrace${CLICKHOUSE_TEST_UNIQUE_NAME}"
OUT_A=$(${CLICKHOUSE_CLIENT} --query "$(query "http://leakuser:${SECRET_A}@127.0.0.1:1/")" 2>&1)
echo "$OUT_A" | grep -F 'Fetching schema id' \
    | assert_shape "trace_log" "$SECRET_A" "Fetching schema id = 1 from url http://[HIDDEN]@127.0.0.1:1/schemas/ids/1"
echo "$OUT_A" | count_secret_in_server_output "$SECRET_A"

# 2. The error for a scheme the HTTP connection pool does not support.
SECRET_B="canaryScheme${CLICKHOUSE_TEST_UNIQUE_NAME}"
OUT_B=$(${CLICKHOUSE_CLIENT} --query "$(query "ftp://leakuser:${SECRET_B}@127.0.0.1:1/")" 2>&1)
echo "$OUT_B" | grep -F 'Unsupported scheme in URI' \
    | assert_shape "unsupported_scheme" "$SECRET_B" "Unsupported scheme in URI 'ftp://[HIDDEN]@127.0.0.1:1/schemas/ids/1'"
echo "$OUT_B" | count_secret_in_server_output "$SECRET_B"
