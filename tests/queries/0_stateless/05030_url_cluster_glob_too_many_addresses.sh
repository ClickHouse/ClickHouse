#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `urlCluster` has to name itself in the "too many result addresses" message on every code path,
# not only when the initiator expands a range.

# Failover options (the `|` separator) survive the initiator intact: the whole group is sent to a
# worker as a single task and is only split there, inside the `StorageURL` created for the
# secondary query, so this exercises the worker-side naming.
failover_uri=$(printf 'http://localhost:1/data-%d.tsv|' {0..10})
failover_uri=${failover_uri%|}
$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', '${failover_uri}', TSV, 'x UInt8') SETTINGS glob_expansion_max_elements = 10" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 11, while at most 10 are allowed" \
    | sort -u

# A range is generated lazily on the initiator, so the limit is hit by reading every address. The
# addresses are served by the HTTP interface of the server the test runs against.
URL="${CLICKHOUSE_URL}&query=SELECT+{0..20}"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM urlCluster('test_shard_localhost', '$URL', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 21, while at most 5 are allowed" \
    | head -n 2

# When the structure is omitted, schema inference on the initiator stops at the first address it can
# read, and the limit is hit by the reading again.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM urlCluster('test_shard_localhost', '$URL', TSV) SETTINGS glob_expansion_max_elements = 5" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 21, while at most 5 are allowed" \
    | head -n 2
