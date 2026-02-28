#!/usr/bin/env bash
# Tags: race

# Regression test for pipeline stuck with window functions and numbers_mt.
# The ResizeProcessor in the scatter-gather topology could get stuck
# due to stale entries in its internal queues when outputs finish
# while the queue still holds references to them.
# https://github.com/ClickHouse/ClickHouse/issues/57728

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

for _ in {1..100}; do
    echo "SELECT sum(a[length(a)]) FROM (SELECT groupArray(number) IGNORE NULLS OVER (PARTITION BY modulo(number, 11) ORDER BY modulo(number, 1111) ASC, number ASC) AS a FROM numbers_mt(10000)) SETTINGS max_block_size = 7;"
done | $CLICKHOUSE_CLIENT -n | grep -vcE '^49995000$' && echo 'Fail!' || echo 'OK'
