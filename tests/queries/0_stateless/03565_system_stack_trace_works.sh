#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# system.stack_trace has an inherent race condition in the mechanism of obtaining the data,
# so it works most of the time in practice, and checking that it works at least sometimes is enough for the test.
for _ in {1..100}
do
    ${CLICKHOUSE_LOCAL} "SELECT count() > 0 FROM system.stack_trace WHERE demangle(addressToSymbol(arrayJoin(trace))) LIKE 'DB::%'" | grep -F '1' && break;
done
