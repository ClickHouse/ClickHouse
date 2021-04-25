#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# NOTE: since 'max_uri_size' doesn't affect the request itself, this test hardly depends on the default value of this setting (16Kb).

LONG_REQUEST=$(python3 -c "print('&max_uri_size=1'*2000, end='')")  # ~30K

${CLICKHOUSE_CURL} -sSv "${CLICKHOUSE_URL}${LONG_REQUEST}&query=SELECT+1" 2>&1 | grep -Fc "HTTP/1.1 400 Bad Request"
