#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that `compression_method='snappy'` round-trips through `file()` for both
# values of the `snappy_mode` setting, and that mismatched modes fail to decode.

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

BASIC_FILE="${DIR}/basic.tsv.snappy"
FRAMED_FILE="${DIR}/framed.tsv.snappy"

# Round-trip with the default `snappy_mode='basic'` (Hadoop snappy block format).
${CLICKHOUSE_CLIENT} -q "
INSERT INTO TABLE FUNCTION file('${BASIC_FILE}', 'TSV', 'x UInt32', 'snappy')
SELECT number FROM numbers(5);
"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${BASIC_FILE}', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
"

# Round-trip with `snappy_mode='framed'` (snappy framing format).
${CLICKHOUSE_CLIENT} -q "
INSERT INTO TABLE FUNCTION file('${FRAMED_FILE}', 'TSV', 'x UInt32', 'snappy')
SELECT number FROM numbers(5)
SETTINGS snappy_mode = 'framed';
"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${FRAMED_FILE}', 'TSV', 'x UInt32', 'snappy')
ORDER BY x
SETTINGS snappy_mode = 'framed';
"

# Reading framed-snappy under default `snappy_mode='basic'` must fail with a
# decode error, not silently accept the wrong wire format.
if ${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${FRAMED_FILE}', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
" 2>&1 | grep -qE "SNAPPY_UNCOMPRESS_FAILED|Cannot read all data"
then
    echo "OK: framed payload rejected by basic reader"
else
    echo "FAIL: framed payload was not rejected by basic reader" >&2
    exit 1
fi
