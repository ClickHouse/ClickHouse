#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

for _ in {1..10}; do seq 1 100 | sed 's/.*/SELECT * FROM (SELECT * FROM system.numbers_mt LIMIT 111) LIMIT 55;/' | $CLICKHOUSE_CLIENT -n --max_block_size=1 | wc -l | grep -vE '^5500$' && echo 'Fail!' && break; echo -n '.'; done; echo
