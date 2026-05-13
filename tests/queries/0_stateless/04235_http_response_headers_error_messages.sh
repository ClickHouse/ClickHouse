#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104777
#
# Error messages for the `http_response_headers` setting used to hardcode
# `additional_http_headers` (the name of the C++ local variable) instead of
# the actual user-facing setting name. This test verifies that the public
# name is reported in all error paths that are reachable from SQL.

echo "Duplicate entries:"
${CLICKHOUSE_CURL} -sS --globoff "${CLICKHOUSE_URL}" \
    -d "SELECT 1 SETTINGS http_response_headers = \$\${'a':'b','a':'c'}\$\$" 2>&1 \
    | grep -o -E "(http_response_headers|additional_http_headers)" \
    | sort -u

echo "Control character in value:"
${CLICKHOUSE_CURL} -sS --globoff "${CLICKHOUSE_URL}" \
    -d "SELECT 1 SETTINGS http_response_headers = {'a':'b\nc'}" 2>&1 \
    | grep -o -E "(http_response_headers|additional_http_headers)" \
    | sort -u

echo "Control character in key:"
${CLICKHOUSE_CURL} -sS --globoff "${CLICKHOUSE_URL}" \
    -d "SELECT 1 SETTINGS http_response_headers = {'a\rb':'c'}" 2>&1 \
    | grep -o -E "(http_response_headers|additional_http_headers)" \
    | sort -u
