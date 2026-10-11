#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `url` family expands the failover options (`|`) of every generated address as it is taken. When
# the options of a single address already exceed the limit, the message still reports the number of
# addresses the whole first argument generates: `{1,2}{1|2|3}` is six addresses, not three. The
# addresses are served by the HTTP interface of the server the test runs against.
URL="${CLICKHOUSE_URL}&query=SELECT+{1,2}{1|2|3}"

# Reading. Parallel replicas may rewrite `url` into its cluster counterpart, which changes the surface.
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 2 --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 6, while at most 2 are allowed" \
    | head -n 2

# Schema inference.
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 2 --query "DESC url('$URL', TSV) SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 6, while at most 2 are allowed" \
    | head -n 2

# The tasks of `urlCluster` are counted by the initiator.
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 2 --query "SELECT count() FROM urlCluster('test_shard_localhost', '$URL', TSV, 'x UInt64')" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 6, while at most 2 are allowed" \
    | head -n 2

# With room for all six addresses, one option of each address is read.
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 6 --query "SELECT count() FROM url('$URL', TSV, 'x UInt64')"
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 6 --query "SELECT count() FROM urlCluster('test_shard_localhost', '$URL', TSV, 'x UInt64')"
