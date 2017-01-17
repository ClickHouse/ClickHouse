#!/usr/bin/env bash

set -o errexit
set -o pipefail

for i in {1..10}; do seq 1 100 | sed 's/.*/SELECT count() FROM (SELECT * FROM (SELECT * FROM system.numbers_mt LIMIT 111) LIMIT 55);/' | clickhouse-client -n --receive_timeout=1 --max_block_size=1 | grep -vE '^55$' && echo 'Fail!' && break; echo -n '.'; done; echo
