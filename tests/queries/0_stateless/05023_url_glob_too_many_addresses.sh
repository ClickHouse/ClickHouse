#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `url` family generates the addresses of a pattern lazily, so the limit is hit by a query that
# reads more addresses than allowed. They are served by the HTTP interface of the server the test runs
# against, so that every address can actually be read until the limit is reached.
# Parallel replicas may rewrite `url` into its cluster counterpart, which legitimately changes the
# surface named in the message, so pin the plain code path where the naming is asserted.
URL="${CLICKHOUSE_URL}&query=SELECT+{0..20}"

# A single range that is larger than the limit.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5, enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 21, while at most 5 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

# A direct product: the reported number is the cardinality of the whole pattern, not of one factor.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('${CLICKHOUSE_URL}&query=SELECT+{0..4}{0..4}', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5, enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 25, while at most 5 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

# `remote` has a dedicated setting.
$CLICKHOUSE_CLIENT --query "SELECT * FROM remote('127.0.0.{1..2000}', system.one)" 2>&1 \
    | grep -oF -e "Table function 'remote'" -e "too many result addresses: 2000, while at most 1000 are allowed" -e "'table_function_remote_max_addresses' setting" \
    | head -n 3

# The `URL` table engine names itself when the table is read.
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.url_glob (x UInt64) ENGINE = URL('$URL', TSV)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.url_glob SETTINGS glob_expansion_max_elements = 5" 2>&1 \
    | grep -oF -e "Table engine 'URL'" -e "too many result addresses: 21, while at most 5 are allowed" \
    | head -n 2

# So does `urlCluster`.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM urlCluster('test_shard_localhost', '$URL', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 21, while at most 5 are allowed" \
    | head -n 2

# A pattern within the limit is read in full.
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM url('$URL', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 21"

# Wildcards from HTTP index pages expand the host part of the pattern before any index page is fetched.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost{1..2000}/**/part.tsv', TSV, 'x UInt8') SETTINGS allow_experimental_url_wildcard_from_index_pages = 1" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 2000, while at most 1000 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

$CLICKHOUSE_CLIENT --allow_experimental_url_wildcard_from_index_pages 1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.url_index_pages_glob (x UInt8) ENGINE = URL('http://localhost{1..2000}/**/part.tsv', TSV)" 2>&1 \
    | grep -oF -e "Table engine 'URL'" -e "too many result addresses: 2000, while at most 1000 are allowed" \
    | head -n 2
