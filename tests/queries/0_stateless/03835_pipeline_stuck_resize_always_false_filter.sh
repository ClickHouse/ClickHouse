#!/usr/bin/env bash
# Tags: race

# Regression test for pipeline stuck with always-false filter after Resize processor.
# The ResizeProcessor could get stuck returning PortFull due to stale entries
# in its internal waiting_outputs queue when downstream FilterTransform
# finishes immediately (always_false).
# https://github.com/ClickHouse/ClickHouse/issues/57728

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

for _ in {1..100}; do
    echo "SELECT count() FROM (SELECT number FROM numbers(100) GROUP BY number) WHERE 0 SETTINGS max_threads = 2;"
    echo "SELECT count() FROM (SELECT number FROM numbers(100) GROUP BY number) WHERE 0 SETTINGS max_threads = 3;"
    echo "SELECT count() FROM (SELECT number FROM numbers(100) GROUP BY number) WHERE 0 SETTINGS max_threads = 4;"
    echo "SELECT count() FROM (SELECT number FROM numbers(100) GROUP BY number) WHERE 0 SETTINGS max_threads = 5;"
done | $CLICKHOUSE_CLIENT -n | grep -vcE '^0$' && echo 'Fail!' || echo 'OK'
