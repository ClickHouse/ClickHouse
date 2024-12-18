#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

start=$SECONDS
# If the memory leak exists, it will lead to OOM fairly quickly.
for _ in {1..1000}; do
    $CLICKHOUSE_CLIENT --max_memory_usage 1G --max_rows_to_read 0 <<< "SELECT uniqExactState(number) FROM system.numbers_mt GROUP BY number % 10";

    # NOTE: we cannot use timeout here since this will not guarantee that the query will be executed at least once.
    # (since graceful wait of clickhouse-client had been reverted)
    elapsed=$((SECONDS - start))
    if [[ $elapsed -gt 30 ]]; then
        break
    fi
done 2>&1 | grep -o -P 'Query memory limit exceeded' | sed -r -e 's/(.*):([a-Z ]*)([mM]emory limit exceeded)(.*)/\2\3/' | uniq
echo 'Ok'
