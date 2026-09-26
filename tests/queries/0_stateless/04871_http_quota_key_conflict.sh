#!/usr/bin/env bash
# A quota key given as both the X-ClickHouse-Quota header and the quota_key query
# parameter is a conflict, not a silent override: authenticateUserByHTTP rejects it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&quota_key=04871_url" \
    -H 'X-ClickHouse-Quota: 04871_header' -d 'SELECT 1' | grep -o 'Code: 36'
