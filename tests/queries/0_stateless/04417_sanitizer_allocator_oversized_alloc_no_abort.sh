#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# An oversized allocation used to abort the sanitizer runtime ("Server died"). With
# allocator_may_return_null=1 it returns null instead, so clickhouse-local stays alive.
# The benign "<Sanitizer> failed to allocate ... bytes" warning is filtered out.
$CLICKHOUSE_LOCAL --query \
    "SYSTEM ALLOCATE MEMORY 999999999999999999; SELECT 'server alive'" \
    2> >(grep -v 'failed to allocate' >&2)
