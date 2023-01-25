#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

terra_ipv6="2600:1419:c400::214:c410"

i=0 retries=300
while [[ $i -lt $retries ]]; do
    $CLICKHOUSE_CLIENT -q "select reverseDNSQuery('${terra_ipv6}')"
    ((++i))
done
