#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -o errexit
set -o pipefail

for i in {1..10}; do seq 1 100 | sed 's/.*/SELECT * FROM system.numbers_mt LIMIT 111;/' | $CLICKHOUSE_CLIENT -n --receive_timeout=1 --max_block_size=$(($RANDOM % 123 + 1)) | wc -l | grep -vE '^11100$' && echo 'Fail!' && break; echo -n '.'; done; echo
