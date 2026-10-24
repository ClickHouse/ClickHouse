#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A pattern may generate shards (`,`) and replicas (`|`) at the same time, and the limit is on the
# number of addresses the whole first argument generates, not on what each of the two expansion
# stages generates on its own: `example01-0{1,2}-{1|2}` is two shards with two replicas each, four
# addresses in total. The number in the message is that total as well.
# Parallel replicas may rewrite `url` into its cluster counterpart, which changes the surface, so
# pin the plain code path.

# Neither stage exceeds the limit on its own.
$CLICKHOUSE_CLIENT --table_function_remote_max_addresses 3 --query "SELECT * FROM remote('example01-0{1,2}-{1|2}', system.one)" 2>&1 \
    | grep -oF -e "Table function 'remote'" -e "too many result addresses: 4, while at most 3 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --table_function_remote_max_addresses 3 --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_mixed_glob ENGINE = Remote('example01-0{1,2}-{1|2}', 'default')" 2>&1 \
    | grep -oF -e "Database engine 'Remote'" -e "too many result addresses: 4, while at most 3 are allowed" \
    | head -n 2

# The `url` family generates the shards lazily and expands the replicas of each one as it is taken, so
# the total is counted as the addresses are consumed, and the limit is hit by reading them all. The
# addresses are served by the HTTP interface of the server the test runs against: `SELECT+1{1|2}` is
# a shard with the replicas `SELECT+11` and `SELECT+12`.
URL="${CLICKHOUSE_URL}&query=SELECT+{1,2}{1|2}"
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 3 --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 4, while at most 3 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.url_mixed_glob (x UInt64) ENGINE = URL('$URL', TSV)"
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 3 --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.url_mixed_glob" 2>&1 \
    | grep -oF -e "Table engine 'URL'" -e "too many result addresses: 4, while at most 3 are allowed" \
    | head -n 2

# With room for all four addresses, one replica of each shard is read.
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 4 --query "SELECT count() FROM url('$URL', TSV, 'x UInt64')"

# The HTTP index pages path expands the same way; the limit is crossed by the second shard, but the
# reported number covers all four of them: 4 * 2.
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 3 --query "SELECT * FROM url('http://localhost{1,2}-{1|2},localhost{3,4}-{1|2}/**/part.tsv', TSV, 'x UInt8') SETTINGS allow_experimental_url_wildcard_from_index_pages = 1" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 8, while at most 3 are allowed" \
    | head -n 2

# The shard stage exceeds the limit before the replicas are looked at: 2000 * 2, not 2000.
$CLICKHOUSE_CLIENT --query "SELECT * FROM remote('127.0.0.{1..2000}-{1|2}', system.one)" 2>&1 \
    | grep -oF "too many result addresses: 4000, while at most 1000 are allowed" \
    | head -n 1

