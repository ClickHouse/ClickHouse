#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

for _ in {1..10}; do seq 1 100 | sed 's/.*/SELECT count() FROM (SELECT * FROM (SELECT * FROM system.numbers_mt LIMIT 111) LIMIT 55);/' | $CLICKHOUSE_CLIENT -n --max_block_size=1 | grep -vE '^55$' && echo 'Fail!' && break; echo -n '.'; done; echo
