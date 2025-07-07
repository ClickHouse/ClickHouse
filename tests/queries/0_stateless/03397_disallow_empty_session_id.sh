#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This should fail
${CLICKHOUSE_CURL} -X GET -sS "${CLICKHOUSE_URL}&session_id=&query=SELECT+1" | grep -o "SESSION_ID_EMPTY"
