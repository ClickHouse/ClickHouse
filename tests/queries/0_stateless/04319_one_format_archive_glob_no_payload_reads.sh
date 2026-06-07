#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Password-protected archive holding two files. `One` must list every file
# matched by the glob without decrypting payloads; reading payloads would
# require the password and fail.
archive_path="${CLICKHOUSE_TEST_UNIQUE_NAME}_one_format_archive_glob.zip"
rm -f "$archive_path"

cat <<'EOF' | base64 --decode > "$archive_path"
UEsDBAoACQAAAAAAIVyXKlcYEAAAAAQAAAAFAAAAeC50c3avq5g3Tsghza+M1t4sJBHwUEsHCJcqVxgQAAAABAAAAFBLAwQKAAkAAAAAACFciFeeNBIAAAAGAAAABQAAAHkudHN2Y+nH6Fd7Qcr287sqEJtQAKIIUEsHCIhXnjQSAAAABgAAAFBLAQIeAwoACQAAAAAAIVyXKlcYEAAAAAQAAAAFAAAAAAAAAAEAAACkgQAAAAB4LnRzdlBLAQIeAwoACQAAAAAAIVyIV540EgAAAAYAAAAFAAAAAAAAAAEAAACkgUMAAAB5LnRzdlBLBQYAAAAAAgACAGYAAACIAAAAAAA=
EOF

$CLICKHOUSE_LOCAL -q "select 'one', _file, count() from file('$archive_path :: {x,y}.tsv', 'One') group by _file order by _file"
$CLICKHOUSE_LOCAL -q "select 'tsv', count() from file('$archive_path :: x.tsv', 'TSV', 'c String')" 2>&1 | grep -o "Password is required"

rm -f "$archive_path"
