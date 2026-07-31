#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

archive_path="${CLICKHOUSE_TEST_UNIQUE_NAME}_one_format_archive.zip"
rm -f "$archive_path"

cat <<'EOF' | base64 --decode > "$archive_path"
UEsDBAoACQAAAK1mtlwox4tQKgAAAB4AAAALABwAcGF5bG9hZC50c3ZVVAkAA0XhD2pF4Q9qdXgLAAEE9gEAAAQUAAAA7IOw2/W3XdtkPVMl9dw9IMB6NEetetrVVRIbdEBc/ytad8YQPX7uhOanUEsHCCjHi1AqAAAAHgAAAFBLAQIeAwoACQAAAK1mtlwox4tQKgAAAB4AAAALABgAAAAAAAEAAACkgQAAAABwYXlsb2FkLnRzdlVUBQADReEPanV4CwABBPYBAAAEFAAAAFBLBQYAAAAAAQABAFEAAAB/AAAAAAA=
EOF

$CLICKHOUSE_LOCAL -q "select 'one', _file, count() from file('$archive_path :: payload.tsv', 'One') group by _file"
$CLICKHOUSE_LOCAL -q "select 'tsv', count() from file('$archive_path :: payload.tsv', 'TSV', 'x String')" 2>&1 | grep -o "Password is required"
$CLICKHOUSE_LOCAL -q "select 'missing', count() from file('$archive_path :: missing.tsv', 'One')"

rm -f "$archive_path"
