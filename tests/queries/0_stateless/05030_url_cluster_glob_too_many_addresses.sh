#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `urlCluster` has to name itself in the "too many result addresses" message on every code path,
# not only when the initiator expands a range. Patterns are expanded before anything is fetched,
# so nothing here reaches the network.

# Failover options (the `|` separator) survive the initiator intact: the whole group is sent to a
# worker as a single task and is only split there, inside the `StorageURL` created for the
# secondary query, so this exercises the worker-side naming.
failover_uri=$(printf 'http://localhost:1/data-%d.tsv|' {0..10})
failover_uri=${failover_uri%|}
$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', '${failover_uri}', TSV, 'x UInt8') SETTINGS glob_expansion_max_elements = 10" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 11, while at most 10 are allowed" \
    | sort -u

# When the structure is omitted, the addresses are expanded during schema inference on the
# initiator, before any storage is created.
$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', 'http://localhost:1/data-{0..2000}.tsv', TSV)" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 2001, while at most 1000 are allowed" \
    | head -n 2

# The same with the format omitted as well: format detection takes a different branch of the
# schema inference.
$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', 'http://localhost:1/data-{0..2000}')" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 2001, while at most 1000 are allowed" \
    | head -n 2
