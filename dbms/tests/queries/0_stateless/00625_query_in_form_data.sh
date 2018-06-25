#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} ${CLICKHOUSE_URL}?query="select" -X POST -F "query= 1;" 2>/dev/null
