#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-fasttest: around 10 seconds of blocking execution

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# If initialization of clickhouse-client or clickhouse-local is slow, there is a high chance of sporadic failures of other tests,
# that are very difficult to debug. To make it easier, we have this, "canary" test. It will fail earlier and show the problem.

# How many times in ten seconds the app can start, run the query, and finish:
start=$SECONDS; while true; do [[ $((SECONDS - start)) -gt 10 ]] && break; ${CLICKHOUSE_LOCAL} -q 1; done | wc -l |
    ${CLICKHOUSE_LOCAL} --input-format TSV --query "SELECT c1 >= 10 ? 'Ok' : ('Fail: ' || c1) FROM table"
