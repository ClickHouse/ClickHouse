#!/usr/bin/env bash
# Tags: long

# This is the regression for the concurrent access in ProgressIndication,
# so it is important to read enough rows here (10e6).
#
# Initially there was 100e6, but under thread fuzzer 10min may be not enough sometimes,
# but I believe that CI will catch possible issues even with less rows anyway.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

tmp_file_progress="$(mktemp "$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME.XXXXXX.progress")"
trap 'rm $tmp_file_progress' EXIT

yes | head -n10000000 | $CLICKHOUSE_LOCAL -q "insert into function null('foo String') format TSV" --progress 2> "$tmp_file_progress"
echo $?
test -s "$tmp_file_progress" && echo "--progress produce some rows" || echo "FAIL: no rows with --progress"
