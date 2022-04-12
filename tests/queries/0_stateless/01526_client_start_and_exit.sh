#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create a huge amount of tables, so Suggest will take a time to load
${CLICKHOUSE_CLIENT} -q "SELECT 'CREATE TABLE test_' || hex(randomPrintableASCII(40)) || '(x UInt8) Engine=Memory;' FROM numbers(10000)" --format=TSVRaw | ${CLICKHOUSE_BENCHMARK} -c32 -i 10000 -d 0 2>&1 | grep -F 'Loaded 10000 queries'

function stress()
{
    # 2004l is ignored because parallel running expect emulated terminal doesn't
    # work well with bracketed paste enabling sequence, which is \e033?2004l
    # (https://cirw.in/blog/bracketed-paste)
    while true; do
        "${CURDIR}"/01526_client_start_and_exit.expect-not-a-test-case | grep -v -P 'ClickHouse client|Connecting|Connected|:\) Bye\.|new year|^\s*$|spawn bash|\?2004l|^0\s*$'
    done
}

export CURDIR
export -f stress

for _ in {1..10}; do
    timeout 3 bash -c stress &
done

wait
