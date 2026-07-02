#!/usr/bin/env bash

# Test: exercises the conflict path in `authenticateUserByHTTP` when both
# `X-ClickHouse-Quota` HTTP header and `quota_key` URL parameter are supplied.
# Covers: src/Server/HTTP/authenticateUserByHTTP.cpp:241-248
#   if (params.has("quota_key")) { if (!quota_key.empty()) throw BAD_ARGUMENTS; ... }

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Conflict case 1: X-ClickHouse-Quota header + quota_key URL parameter (with X-ClickHouse-User auth scheme)
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&quota_key=urlquota" -H 'X-ClickHouse-User: default' -H 'X-ClickHouse-Quota: headerquota' -d 'SELECT 1' | grep -o 'Code: 36'

# Conflict case 2: X-ClickHouse-Quota header + quota_key URL parameter (no other auth headers — Basic credentials path)
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&quota_key=urlquota" -H 'X-ClickHouse-Quota: headerquota' -d 'SELECT 1' | grep -o 'Code: 36'

# Sanity: quota_key URL parameter alone (no header) must work.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&quota_key=urlquota_only" -d 'SELECT 1'

# Sanity: X-ClickHouse-Quota header alone (no URL parameter) must work.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Quota: headerquota_only' -d 'SELECT 1'
