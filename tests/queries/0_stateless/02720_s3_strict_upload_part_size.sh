#!/usr/bin/env bash
# Tags: no-fasttest, long
# Tag no-fasttest: requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

in="$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME.in"
out="$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME.out"
log="$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME.log"

set -e
trap 'rm -f "${out:?}" "${in:?}" "${log:?}"' EXIT

# Generate a file of 20MiB in size, with our part size it will have 4 parts
# NOTE: 1 byte is for new line, so 1023 not 1024
$CLICKHOUSE_LOCAL -q "SELECT randomPrintableASCII(1023) FROM numbers(20*1024) FORMAT LineAsString" > "$in"

$CLICKHOUSE_CLIENT --send_logs_level=trace --server_logs_file="$log" -q "INSERT INTO FUNCTION s3(s3_conn, filename='$CLICKHOUSE_TEST_UNIQUE_NAME', format='LineAsString', structure='line String') FORMAT LineAsString" --s3_strict_upload_part_size=6000001 < "$in"
grep -F '<Fatal>' "$log" || :
grep -o 'WriteBufferFromS3: writePart.*, part size: .*' "$log" | grep -o 'part size: .*'
$CLICKHOUSE_CLIENT -q "SELECT * FROM s3(s3_conn, filename='$CLICKHOUSE_TEST_UNIQUE_NAME', format='LineAsString', structure='line String') FORMAT LineAsString" > "$out"

diff -q "$in" "$out"
