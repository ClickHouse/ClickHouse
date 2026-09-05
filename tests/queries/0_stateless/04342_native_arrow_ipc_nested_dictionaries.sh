#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the native Arrow writer encoding nested LowCardinality columns as Arrow dictionaries
# (output_format_arrow_low_cardinality_as_dictionary=1): a LowCardinality inside Array/Tuple/Map must be
# written as a nested Arrow Dictionary (its own id + DictionaryBatch), not materialized to plain values.
# The native-written file must read back identically through the native reader, for both the Arrow (file)
# and ArrowStream formats, and `pyarrow` - an independent Arrow implementation - must see the nested
# dictionary encoding in the emitted schema, so that a bug shared by the native writer and reader
# cannot hide.

DATA_FILE="${CLICKHOUSE_TMP}/04342_nested_dict"

for FMT in ArrowStream Arrow; do
    ${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}.${FMT}', '${FMT}')
    SELECT
        toLowCardinality(toString(number % 3)) AS lc,
        [toLowCardinality(toString(number % 4)), toLowCardinality(toString(number % 2))] AS arr_lc,
        tuple(toLowCardinality(toString(number % 5)), number) AS tup_lc,
        map(toLowCardinality(toString(number % 6)), number) AS map_lc,
        if(number % 2, NULL, toLowCardinality(toString(number % 7)))::LowCardinality(Nullable(String)) AS lc_null
    FROM numbers(30)
    SETTINGS output_format_arrow_string_as_string = 1,
             output_format_arrow_low_cardinality_as_dictionary = 1,
             output_format_arrow_compression_method = 'none',
             engine_file_truncate_on_insert = 1
    "

    # Print the row multiset (sort the rows): the row order within equal `(lc, arr_lc)` groups is not
    # deterministic under the randomized thread/block settings the test runner injects, so do not
    # depend on it.
    echo "--- ${FMT} ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}.${FMT}', '${FMT}')" | sort

    # An independent Arrow implementation must see the nested dictionary encoding and the same values.
    echo "--- ${FMT} read by pyarrow ---"
    python3 - "${DATA_FILE}.${FMT}" "${FMT}" <<'PY'
import sys
import pyarrow as pa

path, fmt = sys.argv[1], sys.argv[2]
with pa.OSFile(path, "rb") as source:
    opener = pa.ipc.open_file if fmt == "Arrow" else pa.ipc.open_stream
    table = opener(source).read_all()


def norm(value):
    """Print values in a way that does not depend on the Python objects a `pyarrow` release returns."""
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return value.hex().upper()
    if isinstance(value, dict):
        return "{" + ",".join(f"{norm(k)}:{norm(v)}" for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(norm(v) for v in value) + "]"
    return str(value)


for field in table.schema:
    print(field.name, field.type, sep="\t")
print(*sorted(norm(list(row.values())) for row in table.to_pylist()), sep="\n")
PY

    rm -f "${DATA_FILE}.${FMT}"
done

echo "--- inferred schema of native-written nested dictionaries (native reader) ---"
${CLICKHOUSE_LOCAL} --query "
INSERT INTO FUNCTION file('${DATA_FILE}.ArrowStream', 'ArrowStream')
SELECT
    toLowCardinality(toString(number)) AS lc,
    [toLowCardinality(toString(number))] AS arr_lc
FROM numbers(3)
SETTINGS output_format_arrow_string_as_string = 1, output_format_arrow_low_cardinality_as_dictionary = 1,
         output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1
"
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DATA_FILE}.ArrowStream', 'ArrowStream')"

echo "--- values (native) ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}.ArrowStream', 'ArrowStream') ORDER BY lc"

rm -f "${DATA_FILE}.ArrowStream"
