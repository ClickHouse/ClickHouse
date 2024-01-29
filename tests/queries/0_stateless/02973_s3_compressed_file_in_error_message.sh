#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select * from s3('http://localhost:11111/test/a.tsv', TSV, 'x String', 'gzip')" 2>&1 | grep -c -F "a.tsv"

