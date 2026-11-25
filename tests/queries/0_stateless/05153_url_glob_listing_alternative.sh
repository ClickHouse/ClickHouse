#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The "too many result addresses" message of the `url` family explains that HTTP cannot list the
# existing files and recommends an object storage surface instead. The recommendation has to be of
# the same kind as the surface that was invoked: a table engine cannot be replaced by a table
# function, and `urlCluster` needs a clustered replacement.
# The addresses are generated lazily, so they are served by the HTTP interface of the server the test
# runs against, and the limit is hit by reading them all.
# Parallel replicas may rewrite `url` into its cluster counterpart, which changes the surface, so
# pin the plain code path.
URL="${CLICKHOUSE_URL}&query=SELECT+{0..20}"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5, enable_parallel_replicas = 0" 2>&1 \
    | grep -oF "Use 's3' (or another object storage table function) if" \
    | head -n 1

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.url_glob_alternative (x UInt64) ENGINE = URL('$URL', TSV)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.url_glob_alternative SETTINGS glob_expansion_max_elements = 5" 2>&1 \
    | grep -oF "Use 'S3' (or another object storage table engine) if" \
    | head -n 1

$CLICKHOUSE_CLIENT --query "SELECT count() FROM urlCluster('test_shard_localhost', '$URL', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5" 2>&1 \
    | grep -oF "Use 's3Cluster' (or another object storage cluster table function) if" \
    | head -n 1

# A group of alternatives is part of the direct product, wherever it stands: 2 * 21.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('${CLICKHOUSE_URL}&query=SELECT+{1,2}{0..20}', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5, enable_parallel_replicas = 0" 2>&1 \
    | grep -oF "too many result addresses: 42, while at most 5 are allowed" \
    | head -n 1

$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('${CLICKHOUSE_URL}&query=SELECT+{0..20}{1,2}', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5, enable_parallel_replicas = 0" 2>&1 \
    | grep -oF "too many result addresses: 42, while at most 5 are allowed" \
    | head -n 1

# `remote` can list nothing either, but there is no object storage replacement for it.
$CLICKHOUSE_CLIENT --query "SELECT * FROM remote('127.0.0.{1..2000}', system.one)" 2>&1 \
    | grep -cF "object storage" || true
