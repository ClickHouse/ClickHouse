#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -v "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress" -d 'SELECT number FROM numbers(10)' 2>&1 | grep -P 'X-ClickHouse-Format'
