#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `INTO OUTFILE ... COMPRESSION 'snappy'` must work for both `snappy_mode` values:
#  - `framed` writes the snappy framing format;
#  - the default `basic` writes the Hadoop snappy block format;
# and each must round-trip through `file()` under the matching mode. The `snappy_mode`
# setting has to be threaded through the client-side write path so it takes effect.

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

FRAMED_FILE="${DIR}/framed.tsv.snappy"
BASIC_FILE="${DIR}/basic.tsv.snappy"

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

# `INTO OUTFILE` with the default `basic` mode writes the Hadoop snappy block
# format, and the result must round-trip through `file()` under the same (default) mode.
${CLICKHOUSE_CLIENT} -q "
SELECT number FROM numbers(5)
INTO OUTFILE '${BASIC_FILE}' COMPRESSION 'snappy' FORMAT TSV;
"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${BASIC_FILE}', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
"
