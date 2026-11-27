#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select count() from url('http://localhost:11111/test%2Fa.tsv') settings enable_url_encoding=1"

# Grep 'test%2Fa.tsv' to ensure that path wasn't encoded/decoded
$CLICKHOUSE_CLIENT -q "select count() from url('http://localhost:11111/test%2Fa.tsv') settings enable_url_encoding=0" 2>&1 | \
 grep -o "test%2Fa.tsv" -m1 | head -n 1
