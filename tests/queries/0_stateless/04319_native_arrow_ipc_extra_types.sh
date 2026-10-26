#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests additional types in the native Arrow IPC reader/writer: UUID (self-describing Arrow extension
# type, byte-swapped), IPv4, IPv6 and big integers. UUID must round-trip with its type and value
# preserved; the others must be writable and read back consistently.
#
# The natively written file is also decoded with `pyarrow`, an independent Arrow implementation, so that
# the UUID extension metadata and the fixed-size-binary layout of `IPv6` / `Int128` / `Int256` stay
# externally validated rather than only checked against the native reader itself.

DATA_FILE="${CLICKHOUSE_TMP}/04319.arrows"

# Native writer; the native reader must keep the UUID column's type and value.
${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream') SELECT
    toUUID('00112233-4455-6677-8899-aabbccddeeff') AS u,
    toIPv4('1.2.3.4') AS ip4,
    toIPv6('2001:db8::1') AS ip6,
    toInt128(number) * 100000000000000000 AS i128,
    toInt256(number) AS i256
FROM numbers(3)
SETTINGS output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1"

echo "--- schema (native) ---"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}', 'ArrowStream')"

echo "--- UUID value (native reader) ---"
${CLICKHOUSE_LOCAL} --query "SELECT DISTINCT u FROM file('${DATA_FILE}', 'ArrowStream')"

echo "--- schema and values as seen by pyarrow ---"
python3 - "${DATA_FILE}" <<'PY'
import sys
import pyarrow as pa

with pa.OSFile(sys.argv[1], "rb") as source:
    table = pa.ipc.open_stream(source).read_all()

def resolve(field):
    """Extension name and storage type, however this pyarrow version surfaces `arrow.uuid`."""
    if isinstance(field.type, pa.BaseExtensionType):
        return field.type.extension_name, field.type.storage_type
    name = (field.metadata or {}).get(b"ARROW:extension:name", b"").decode()
    return name, field.type

for field in table.schema:
    extension, storage = resolve(field)
    print(field.name, storage, extension, sep="\t")

# Print the raw storage bytes, so the assertion is on the layout itself rather than on how a
# particular pyarrow version chooses to box an extension value.
for field in table.schema:
    column = table.column(field.name)
    if isinstance(field.type, pa.BaseExtensionType):
        column = column.cast(field.type.storage_type)
    value = column.slice(1, 1).to_pylist()[0]
    print(field.name, value.hex() if isinstance(value, bytes) else value, sep="\t")
PY

rm -f "${DATA_FILE}"
