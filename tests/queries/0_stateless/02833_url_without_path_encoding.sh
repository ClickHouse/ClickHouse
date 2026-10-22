#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "select count() from url('http://localhost:11111/test%2Fa.tsv') settings enable_url_encoding=1"

# Grep 'test%2Fmissing.tsv' in the error message to ensure that the path wasn't encoded/decoded.
# The object must not exist: S3 servers that URL-decode the raw path (e.g. RustFS, like AWS S3)
# would serve an existing object and return no error to grep.
$CLICKHOUSE_CLIENT -q "select count() from url('http://localhost:11111/test%2Fmissing.tsv') settings enable_url_encoding=0" 2>&1 | \
 grep -o "test%2Fmissing.tsv" -m1 | head -n 1
