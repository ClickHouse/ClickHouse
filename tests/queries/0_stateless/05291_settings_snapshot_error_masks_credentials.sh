#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

canary="settings_snapshot_error_password"
malformed_uri="http://user:${canary}@["

check_error()
{
    local label="$1"
    local query="$2"
    local response
    # HTTP returns only the server's response, without the client's unmasked query echo.
    response=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$query")
    if [[ "$response" != *"BAD_ARGUMENTS"* || "$response" != *"format_avro_schema_registry_url"* || "$response" != *"[HIDDEN]"* ]]; then
        echo "Missing setting name, error code, or masked credential: $label"
        exit 1
    fi
    if [[ "$response" == *"$canary"* ]]; then
        echo "Credential leaked: $label"
        exit 1
    fi
    echo "$label: masked"
}

# SQL validates values before applying them to the `Context`. The direct application and
# cached login-resolution error paths are covered in `gtest_settings_context_errors.cpp`.
check_error "SET" "SET format_avro_schema_registry_url = '${malformed_uri}'"
check_error "SETTINGS" "SELECT 1 SETTINGS format_avro_schema_registry_url = '${malformed_uri}'"
