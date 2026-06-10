#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format is not available in fasttest builds

# The `Arrow` (file) format needs random access. When the input is not seekable the reader loads it
# into memory; doing so must not abort the server (a previous `assert_cast<SeekableReadBuffer *>` on the
# memory buffer aborted in debug/sanitizer builds). Read an Arrow file from a pipe to exercise that path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/05000_native_arrow_ipc_non_seekable.arrow"

${CLICKHOUSE_LOCAL} -q "INSERT INTO TABLE FUNCTION file('${DATA_FILE}', 'Arrow', 'a UInt32, b String') SELECT number, toString(number) FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1"

# Pipe the file into clickhouse-local so the input read buffer is a non-seekable pipe.
cat "${DATA_FILE}" | ${CLICKHOUSE_LOCAL} --input-format Arrow --structure 'a UInt32, b String' -q "SELECT * FROM table ORDER BY a"

rm -f "${DATA_FILE}"
