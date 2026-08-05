#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o pipefail

# test via http, since client parses settings by itself additionally

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT DISTINCT blockSize() FROM numbers(2) SETTINGS max_block_size = 1 FORMAT CSV'
# push down
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT DISTINCT blockSize() FROM numbers(2) FORMAT CSV SETTINGS max_block_size = 1'
# push down append
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT DISTINCT blockSize() FROM numbers(2) SETTINGS max_compress_block_size = 1 FORMAT CSV SETTINGS max_block_size = 1'
# not overwrite on push down
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT DISTINCT blockSize() FROM numbers(2) SETTINGS max_block_size = 2 FORMAT CSV SETTINGS max_block_size = 1'
# on push-down
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT DISTINCT blockSize() FROM numbers(2) SETTINGS max_block_size = 1 FORMAT CSV'
# no settings
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT DISTINCT blockSize() FROM numbers(2) FORMAT CSV'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT DISTINCT blockSize() FROM numbers(2)'
