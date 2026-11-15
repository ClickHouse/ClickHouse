#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression for the client-side stdin input snappy read path: the `snappy_mode`
# setting must be threaded through `ClientBase::sendDataFromStdin` to the
# decompression wrapper. Previously the setting was discarded on the input APIs,
# so a stdin stream could only ever be decoded as the default `basic` (Hadoop
# block) format and framed-snappy input silently failed even when the user set
# `snappy_mode = 'framed'`.

FRAMED_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.framed.tsv.snappy"
trap 'rm -f "${FRAMED_FILE}"' EXIT

# Produce a framed-snappy file with the (working) client-side write path.
# `INTO OUTFILE` refuses to overwrite, so drop any leftover from an earlier run.
rm -f "${FRAMED_FILE}"
${CLICKHOUSE_CLIENT} -q "
SELECT number FROM numbers(5)
INTO OUTFILE '${FRAMED_FILE}' COMPRESSION 'snappy' FORMAT TSV
SETTINGS snappy_mode = 'framed';
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS snappy_stdin"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE snappy_stdin (x UInt32) ENGINE = Memory"

# Compression is auto-detected from the redirected stdin file name (`*.snappy`),
# and the framing must come from `snappy_mode = 'framed'`. With the setting
# threaded through, the framed stream decodes and round-trips.
${CLICKHOUSE_CLIENT} --query "INSERT INTO snappy_stdin SETTINGS snappy_mode = 'framed' FORMAT TSV" < "${FRAMED_FILE}"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM snappy_stdin ORDER BY x"

# Reading the same framed stream under the default `snappy_mode = 'basic'` must
# fail with a decode error, not silently accept the wrong wire format.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE snappy_stdin"
if ${CLICKHOUSE_CLIENT} --query "INSERT INTO snappy_stdin FORMAT TSV" < "${FRAMED_FILE}" 2>&1 \
    | grep -qE "SNAPPY_UNCOMPRESS_FAILED|Cannot read all data"
then
    echo "OK: framed stdin rejected by basic reader"
else
    echo "FAIL: framed stdin was not rejected by basic reader" >&2
    exit 1
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE snappy_stdin"
