#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An Arrow column tagged `clickhouse.opaque` is read back by deserializing each value into the ClickHouse
# type the tag names, so a payload that does not match that type has to be rejected instead of arriving as
# a shorter value that happens to parse. The tag also only applies with the type name attached to it.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"

${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${FILE}', 'ArrowStream') SELECT '{\"a\":1}'::JSON AS j
    SETTINGS output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1,
             output_format_arrow_unsupported_types = 'binary'"

python3 - "${FILE}" <<'PY'
import sys
import pyarrow as pa

with pa.OSFile(sys.argv[1], "rb") as source:
    table = pa.ipc.open_stream(source).read_all()

field = table.schema.field("j")
values = table.column("j").to_pylist()


def write(suffix, payload, schema):
    with pa.OSFile(f"{sys.argv[1]}.{suffix}", "wb") as out:
        with pa.ipc.new_stream(out, schema) as writer:
            writer.write_table(pa.Table.from_arrays([pa.array(payload, type=pa.binary())], schema=schema))


write("trailing", [value + b"\x00" for value in values], table.schema)

metadata = {k: v for k, v in field.metadata.items() if k != b"ARROW:extension:metadata"}
write("untyped", values, pa.schema([field.with_metadata(metadata)]))
PY

echo "--- one byte too many for the tagged type ---"
${CLICKHOUSE_LOCAL} --query "SELECT j FROM file('${FILE}.trailing', 'ArrowStream', 'j JSON')" 2>&1 |
    grep -oF 'tagged as JSON but row 0 has 1 trailing byte(s)' | head -1

echo "--- tag without the ClickHouse type name is not acted on ---"
${CLICKHOUSE_LOCAL} --query "SELECT j FROM file('${FILE}.untyped', 'ArrowStream', 'j JSON')" 2>&1 |
    grep -oaF 'while converting column `j` from type String to type JSON' | head -1

rm -f "${FILE}"*
