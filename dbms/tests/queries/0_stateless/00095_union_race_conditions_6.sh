#!/usr/bin/env bash

set -o errexit
set -o pipefail

for i in {1..10}; do sleep 0.0$RANDOM; seq 1 100 | sed 's/.*/SELECT 1 % (number - 10000000) FROM system.numbers_mt;/' | clickhouse-client -n --receive_timeout=1 --max_block_size=10 >/dev/null 2>&1 && echo 'Fail!' && break; echo -n '.'; done; echo
