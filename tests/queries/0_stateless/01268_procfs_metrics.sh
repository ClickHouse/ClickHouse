#!/usr/bin/env bash

# Sandbox does not provide CAP_NET_ADMIN capability but does have ProcFS mounted at /proc
# This ensures that OS metrics can be collected


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --profile-events-delay-ms=-1 --print-profile-events -q "SELECT * FROM file('${BASH_SOURCE[0]}', 'LineAsString') FORMAT Null" |& {
    grep -F -o -e OSCPUVirtualTimeMicroseconds -e OSReadChars
} | sort | uniq
