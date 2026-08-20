#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Patterns are expanded before anything is fetched, so nothing here reaches the network.

# A single range that is larger than the limit.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{0..2000}.tsv', TSV, 'x UInt8')" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 2001, while at most 1000 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

# A direct product of two ranges: neither of them alone exceeds the limit.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{0..99}-{0..99}.tsv', TSV, 'x UInt8')" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 10000, while at most 1000 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

# The limit of the `remote` table function is controlled by its own setting.
$CLICKHOUSE_CLIENT --query "SELECT * FROM remote('127.0.0.{1..2000}', system.one)" 2>&1 \
    | grep -oF -e "Table function 'remote'" -e "too many result addresses: 2000, while at most 1000 are allowed" -e "'table_function_remote_max_addresses' setting" \
    | head -n 3

# Raising the setting lets the same pattern through: it now fails while connecting, not while parsing.
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('http://localhost:1/data-{0..2000}.tsv', TSV, 'x UInt8') SETTINGS glob_expansion_max_elements = 3000, http_max_tries = 1, max_threads = 1" 2>&1 \
    | grep -cF "too many result addresses" || true
