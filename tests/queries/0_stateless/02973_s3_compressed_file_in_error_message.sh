#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=none -q "select * from s3('http://localhost:11111/test/a.tsv', TSV, 'x String', 'gzip')" 2>&1 | grep -c "(in file/uri.*a\.tsv)"
