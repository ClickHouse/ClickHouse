#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Apache Arrow library reader passes the numeric type hint down recursively, so an Arrow `date32`
# nested in a list/struct/map (or addressed as a subcolumn) and read into a numeric target returns the raw
# day number without the `Date32` range check, including out-of-range days. The native reader must match
# this at every nesting level, and a `date32` mapped to a `Date32` target must still be range-checked.
# Verified against the library reader. Files are built with pyarrow because ClickHouse cannot write an
# out-of-range `date32`.

DATA_FILE="${CLICKHOUSE_TMP}/04346_nested_date32"

python3 - "$DATA_FILE" <<'PY'
import sys
import pyarrow as pa

base = sys.argv[1]
OOR = 3000000  # day number far outside ClickHouse's Date32 range; a numeric target must return it verbatim
shapes = {
    "list": (pa.record_batch([pa.array([[0, OOR], [-25567], []], type=pa.list_(pa.date32()))], names=["arr"])),
    "struct": (pa.record_batch(
        [pa.array([{"d": 0, "n": 1}, {"d": OOR, "n": 2}, {"d": -25567, "n": 3}],
                  type=pa.struct([("d", pa.date32()), ("n", pa.int64())]))], names=["t"])),
    "map": (pa.record_batch([pa.array([[("k", 0)], [("k", OOR)], [("k", -25567)]],
                                      type=pa.map_(pa.string(), pa.date32()))], names=["m"])),
    # Struct whose Arrow field names are upper-cased: with case-insensitive matching, `D` must still map to
    # the requested numeric element `d`, so the nested `date32` is read raw (the hint lookup must respect
    # case-insensitivity rather than fall back to the wrong positional element).
    "ci_struct": (pa.record_batch(
        [pa.array([{"D": 0, "N": 1}, {"D": OOR, "N": 2}, {"D": -25567, "N": 3}],
                  type=pa.struct([("D", pa.date32()), ("N", pa.int64())]))], names=["t"])),
}
for name, batch in shapes.items():
    for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
        with pa.OSFile(f"{base}.{name}.{fmt}", "wb") as sink:
            with opener(sink, batch.schema) as writer:
                writer.write_batch(batch)
PY

# Read column `col` with the given numeric `structure` through both readers; they must agree (and return
# the raw out-of-range day, proving the recursive numeric type hint).
run_pair() {
    local file="$1" fmt="$2" structure="$3" col="$4"
    local native library
    native=$(${CLICKHOUSE_LOCAL}  --query "SELECT ${col} FROM file('${file}', '${fmt}', '${structure}') SETTINGS input_format_arrow_use_native_reader = 1")
    library=$(${CLICKHOUSE_LOCAL} --query "SELECT ${col} FROM file('${file}', '${fmt}', '${structure}') SETTINGS input_format_arrow_use_native_reader = 0")
    if [ "$native" = "$library" ]; then echo "OK native==library | ${col}"; echo "$native"; else echo "MISMATCH | ${col}"; fi
}

for FMT in Arrow ArrowStream; do
    echo "=== ${FMT} ==="
    run_pair "${DATA_FILE}.list.${FMT}"   "${FMT}" "arr Array(Int32)"         "arr"
    run_pair "${DATA_FILE}.struct.${FMT}" "${FMT}" "t Tuple(d Int32, n Int64)" "t"
    run_pair "${DATA_FILE}.struct.${FMT}" "${FMT}" "t Tuple(d Int32, n Int64)" "t.d"
    run_pair "${DATA_FILE}.map.${FMT}"    "${FMT}" "m Map(String, Int32)"      "m"

    echo "--- case-insensitive: Arrow struct field 'D' -> requested 'd Int32' reads raw, native == library ---"
    for COL in t t.d; do
        native=$(${CLICKHOUSE_LOCAL}  --query "SELECT ${COL} FROM file('${DATA_FILE}.ci_struct.${FMT}', '${FMT}', 't Tuple(d Int32, n Int64)') SETTINGS input_format_arrow_use_native_reader = 1, input_format_arrow_case_insensitive_column_matching = 1")
        library=$(${CLICKHOUSE_LOCAL} --query "SELECT ${COL} FROM file('${DATA_FILE}.ci_struct.${FMT}', '${FMT}', 't Tuple(d Int32, n Int64)') SETTINGS input_format_arrow_use_native_reader = 0, input_format_arrow_case_insensitive_column_matching = 1")
        if [ "$native" = "$library" ]; then echo "OK native==library | ${COL}"; echo "$native"; else echo "MISMATCH | ${COL}"; fi
    done

    echo "--- nested Date32 target still range-checks the out-of-range day (native) ---"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT arr FROM file('${DATA_FILE}.list.${FMT}', '${FMT}', 'arr Array(Date32)')
        SETTINGS input_format_arrow_use_native_reader = 1
    " 2>&1 | grep -o "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" | head -1
done

rm -f "${DATA_FILE}".*
