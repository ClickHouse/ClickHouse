#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -o errexit
set -o pipefail

for i in {1..10}; do seq 1 10 | sed 's/.*/SELECT 1 % ((number + 500) % 1000) FROM system.numbers_mt LIMIT 1000;/' | $CLICKHOUSE_CLIENT -n --receive_timeout=1 --max_block_size=1 >/dev/null 2>&1 && echo 'Fail!' && break; echo -n '.'; done; echo
