#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://localhost:11111/test/{a,tsv_with_header}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64')" 2>&1 | grep -o -m1 "http://localhost:11111/test/tsv_with_header.tsv"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM s3('http://localhost:11111/test/{a,tsv_with_header}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64')" 2>&1 | grep -o -m1 "test/tsv_with_header.tsv"
