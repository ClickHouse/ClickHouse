#!/usr/bin/env bash
# Tags: long, no-msan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

export CLICKHOUSE_URL="${CLICKHOUSE_URL}&optimize_in_to_equal=0"
python3 "$CURDIR"/00386_long_in_pk.python

