#!/usr/bin/env bash

# Sandbox does not provide CAP_NET_ADMIN capability but does have ProcFS mounted at /proc
# This ensures that OS metrics can be collected


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# NOTE: netlink taskstruct interface uses rounding to 1KB [1], so we cannot use ${BASH_SOURCE[0]}
#
#   [1]: https://elixir.bootlin.com/linux/v5.18-rc4/source/kernel/tsacct.c#L101
tmp_path=$(mktemp "$CURDIR/01268_procfs_metrics.XXXXXX")
trap 'rm -f $tmp_path' EXIT
truncate -s1025 "$tmp_path"

$CLICKHOUSE_LOCAL --profile-events-delay-ms=-1 --print-profile-events -q "SELECT * FROM file('$tmp_path', 'LineAsString') FORMAT Null" |& grep -m1 -F -o -e OSReadChars
# NOTE: that OSCPUVirtualTimeMicroseconds is in microseconds, so 1e6 is not enough.
$CLICKHOUSE_LOCAL --profile-events-delay-ms=-1 --print-profile-events -q "SELECT * FROM numbers(1e8) FORMAT Null" |& grep -m1 -F -o -e OSCPUVirtualTimeMicroseconds
exit 0
