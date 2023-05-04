#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

start=$SECONDS
for _ in {1..250}; do
    $CLICKHOUSE_CLIENT --query "SELECT groupArrayIfState(('Hello, world' AS s) || s || s || s || s || s || s || s || s || s, NOT throwIf(number > 10000000, 'Ok')) FROM system.numbers_mt GROUP BY number % 10";

    # NOTE: we cannot use timeout here since this will not guarantee that the query will be executed at least once.
    # (since graceful wait of clickhouse-client had been reverted)
    elapsed=$((SECONDS - start))
    if [[ $elapsed -gt 30 ]]; then
        break
    fi
done 2>&1 | grep -o -F 'Ok' | uniq
