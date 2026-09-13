#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Patterns are expanded before anything is fetched, so nothing here reaches the network.
# Parallel replicas may rewrite `url` into its cluster counterpart, which legitimately changes the
# surface named in the message, so pin the plain code path where the naming is asserted.

# A single range that is larger than the limit.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{0..2000}.tsv', TSV, 'x UInt8') SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 2001, while at most 1000 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

# A direct product of two ranges: neither of them alone exceeds the limit.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{0..99}-{0..99}.tsv', TSV, 'x UInt8') SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 10000, while at most 1000 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

# The limit of the `remote` table function is controlled by its own setting.
$CLICKHOUSE_CLIENT --query "SELECT * FROM remote('127.0.0.{1..2000}', system.one)" 2>&1 \
    | grep -oF -e "Table function 'remote'" -e "too many result addresses: 2000, while at most 1000 are allowed" -e "'table_function_remote_max_addresses' setting" \
    | head -n 3

# The `URL` engine and `urlCluster` share the parser with the `url` table function, but the message
# has to name the surface the user actually invoked.
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.url_glob (x UInt8) ENGINE = URL('http://localhost:1/data-{0..2000}.tsv', TSV)" 2>&1 \
    | grep -oF -e "Table engine 'URL'" -e "too many result addresses: 2001, while at most 1000 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', 'http://localhost:1/data-{0..2000}.tsv', TSV, 'x UInt8')" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 2001, while at most 1000 are allowed" \
    | head -n 2

# Raising the setting lets the same pattern through: it now fails while connecting, not while parsing.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{0..2000}.tsv', TSV, 'x UInt8') SETTINGS glob_expansion_max_elements = 3000, http_max_tries = 1, max_threads = 1" 2>&1 \
    | grep -cF "too many result addresses" || true

# A listable `*` wildcard in the path routes the query through `StorageWebConfiguration` and the
# HTTP index pages instead of plain `StorageURL`, and that branch counts the addresses on its own.
# The host template still overflows while the configuration is initialized, before any index page is
# fetched, so this reaches the check without touching the network.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost{1..2000}/**/part.tsv', TSV, 'x UInt8') SETTINGS allow_experimental_url_wildcard_from_index_pages = 1" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 2000, while at most 1000 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

$CLICKHOUSE_CLIENT --allow_experimental_url_wildcard_from_index_pages 1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.url_index_pages_glob (x UInt8) ENGINE = URL('http://localhost{1..2000}/**/part.tsv', TSV)" 2>&1 \
    | grep -oF -e "Table engine 'URL'" -e "too many result addresses: 2000, while at most 1000 are allowed" \
    | head -n 2
