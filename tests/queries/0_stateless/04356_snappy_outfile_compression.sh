#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression for the client-side `INTO OUTFILE` snappy write path: the `snappy_mode`
# setting must be threaded through to the compression wrapper. Previously the setting
# was discarded before this API boundary, so `snappy_mode = 'framed'` had no effect and
# snappy `INTO OUTFILE` always failed even when the user followed the error message.

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

FRAMED_FILE="${DIR}/framed.tsv.snappy"

# `INTO OUTFILE` with framed snappy must succeed, and the result must round-trip
# through `file()` when both sides agree on the framing format.
${CLICKHOUSE_CLIENT} -q "
SELECT number FROM numbers(5)
INTO OUTFILE '${FRAMED_FILE}' COMPRESSION 'snappy' FORMAT TSV
SETTINGS snappy_mode = 'framed';
"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${FRAMED_FILE}', 'TSV', 'x UInt32', 'snappy')
ORDER BY x
SETTINGS snappy_mode = 'framed';
"

# With the default `snappy_mode = 'basic'` there is no snappy block writer, so the
# client must reject the write with an actionable diagnostic that names the setting
# that actually takes effect here, instead of silently ignoring it.
if ${CLICKHOUSE_CLIENT} -q "
SELECT number FROM numbers(5)
INTO OUTFILE '${DIR}/basic.tsv.snappy' COMPRESSION 'snappy' FORMAT TSV;
" 2>&1 | grep -qF "snappy_mode = 'framed'"
then
    echo "OK: basic snappy write rejected with actionable message"
else
    echo "FAIL: basic snappy write not rejected with actionable message" >&2
    exit 1
fi
