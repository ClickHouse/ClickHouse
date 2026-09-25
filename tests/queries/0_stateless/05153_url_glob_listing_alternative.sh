#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The "too many result addresses" message of the `url` family explains that HTTP cannot list the
# existing files and recommends an object storage surface instead. The recommendation has to be of
# the same kind as the surface that was invoked: a table engine cannot be replaced by a table
# function, and `urlCluster` needs a clustered replacement.
# Patterns are expanded before anything is fetched, so nothing here reaches the network.
# Parallel replicas may rewrite `url` into its cluster counterpart, which changes the surface, so
# pin the plain code path.

$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{0..2000}.tsv', TSV, 'x UInt8') SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF "Use 's3' (or another object storage table function) if" \
    | head -n 1

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.url_glob_alternative (x UInt8) ENGINE = URL('http://localhost:1/data-{0..2000}.tsv', TSV)" 2>&1 \
    | grep -oF "Use 'S3' (or another object storage table engine) if" \
    | head -n 1

$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', 'http://localhost:1/data-{0..2000}.tsv', TSV, 'x UInt8')" 2>&1 \
    | grep -oF "Use 's3Cluster' (or another object storage cluster table function) if" \
    | head -n 1

# The number in the message is the cardinality of the whole first argument, not of the single factor
# that made the parser stop: the expanded prefix and the remaining factors count too.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{a,b}-{0..2000}.tsv', TSV, 'x UInt8') SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF "too many result addresses: 4002, while at most 1000 are allowed" \
    | head -n 1

$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{0..2000}-{a,b}.tsv', TSV, 'x UInt8') SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF "too many result addresses: 4002, while at most 1000 are allowed" \
    | head -n 1

# `remote` shares the parser but can enumerate nothing either way, so it gets no object storage hint.
$CLICKHOUSE_CLIENT --query "SELECT * FROM remote('127.0.0.{1..2000}', system.one)" 2>&1 \
    | grep -cF "object storage" || true
