#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "select toString(toIPv6('2001:db9:85a3::8a2e:370:7334'))"
$CLICKHOUSE_CLIENT  --param_var 2001:db8:85a3::8a2e:370:7334 -q "select {var:IPv6}"

