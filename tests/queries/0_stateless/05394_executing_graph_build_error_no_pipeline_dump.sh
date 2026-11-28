#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a failpoint.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A memory limit hit while the executing graph is built is not a malformed pipeline: the error must not carry a dump of it.
output=$(${CLICKHOUSE_LOCAL} --query "SYSTEM ENABLE FAILPOINT executing_graph_build_fail; SELECT number FROM numbers(10) FORMAT Null" 2>&1)
echo "$output" | grep -c 'Failpoint executing_graph_build_fail is triggered' || true
echo "$output" | grep -c 'Query pipeline' || true
