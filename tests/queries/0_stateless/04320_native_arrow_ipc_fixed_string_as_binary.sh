#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: with output_format_arrow_fixed_string_as_fixed_byte_array=0 the schema advertises a
# variable-width Utf8/Binary, so the native writer must emit the offsets+data buffers (not a single
# fixed-width buffer). Otherwise it writes invalid Arrow that a reader misinterprets. Verify the
# native-written data reads back identically with both the native reader and the Apache Arrow library reader.

DATA_FILE="${CLICKHOUSE_TMP}/04320_fs_binary.arrows"

for as_string in 0 1; do
    ${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream')
    SELECT toFixedString(repeat('x', number), 5) AS fs FROM numbers(5)
    SETTINGS output_format_arrow_use_native_writer = 1,
             output_format_arrow_fixed_string_as_fixed_byte_array = 0,
             output_format_arrow_string_as_string = ${as_string},
             engine_file_truncate_on_insert = 1"

    echo "string_as_string=${as_string} native reader:"
    ${CLICKHOUSE_LOCAL} --query "SELECT hex(fs) FROM file('${DATA_FILE}', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 1"
    echo "string_as_string=${as_string} library reader:"
    ${CLICKHOUSE_LOCAL} --query "SELECT hex(fs) FROM file('${DATA_FILE}', 'ArrowStream') SETTINGS input_format_arrow_use_native_reader = 0"
done
