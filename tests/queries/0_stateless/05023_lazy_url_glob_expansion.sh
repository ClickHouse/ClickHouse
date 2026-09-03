#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The addresses are served by the HTTP interface of the server the test runs against: every address of
# the pattern answers with the number it embeds, so reading one address is enough to satisfy a LIMIT.
URL="${CLICKHOUSE_URL}&query=SELECT+{0..19}"

echo "--- reading one address out of twenty, with room for five"
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('$URL', TSV, 'x UInt64') LIMIT 1 SETTINGS glob_expansion_max_elements = 5, max_threads = 1"

echo "--- schema inference reads one address as well"
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('$URL', TSV) LIMIT 1 SETTINGS glob_expansion_max_elements = 5, max_threads = 1"

echo "--- reading all of them is still limited"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 5, max_threads = 1" 2>&1 \
    | grep -oF "too many result addresses" | head -n 1

echo "--- a pattern that fits into the limit is read in full"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM url('$URL', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 20, max_threads = 1"

echo "--- a hundred million addresses under the default limit"
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('${CLICKHOUSE_URL}&query=SELECT+{0..10000}{0..10000}', TSV, 'x UInt64') LIMIT 1 SETTINGS max_threads = 1"

echo "--- a _path predicate is applied to every generated address, which counts against the limit"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') WHERE _path = '/no-such-path' SETTINGS glob_expansion_max_elements = 5, max_threads = 1" 2>&1 \
    | grep -oF "too many result addresses" | head -n 1

echo "--- with room for the whole pattern the predicate prunes every address and nothing is read"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') WHERE _path = '/no-such-path' SETTINGS glob_expansion_max_elements = 20, max_threads = 1"
