#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

for _ in {1..10}; do seq 1 10 | sed 's/.*/SELECT 1 % ((number + 500) % 1000) FROM numbers_mt(1000);/' | $CLICKHOUSE_CLIENT -n --max_block_size=1 >/dev/null 2>&1 && echo 'Fail!' && break; echo -n '.'; done; echo
