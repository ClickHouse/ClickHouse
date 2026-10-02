#!/usr/bin/env bash
# Tags: long, no-fasttest
# This test requires pyarrow and Arrow support.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# `Array`, `Map`, and plain `Tuple` cannot retain a dictionary value array's outer null map.
# A non-nullable encoded field must still reject a row referencing a null dictionary entry,
# while unreferenced null entries and null rows of nullable fields remain legal.
python3 - "$TMP_DIR" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]


def write(name, value_type, batches, nullable=False, deltas=False, formats=("Arrow", "ArrowStream")):
    schema = pa.schema([pa.field("c", pa.dictionary(pa.int32(), value_type), nullable=nullable)])
    options = ipc.IpcWriteOptions(emit_dictionary_deltas=deltas)
    for fmt in formats:
        new_writer = ipc.new_file if fmt == "Arrow" else ipc.new_stream
        with new_writer(f"{out}/{name}.{fmt}", schema, options=options) as writer:
            for values, indices in batches:
                column = pa.DictionaryArray.from_arrays(
                    pa.array(indices, type=pa.int32()), pa.array(values, type=value_type))
                writer.write_batch(pa.record_batch([column], schema=schema))


# A `null`-typed dictionary has no validity buffer, and every entry is null.
write("null_nullable", pa.null(), [([None], [0])], nullable=True)
write("null_referenced", pa.null(), [([None], [0])])


for name, value_type, entries in (
    ("array", pa.list_(pa.int64()), [[11], [33], [55]]),
    ("map", pa.map_(pa.int64(), pa.int64()), [[(11, 111)], [(33, 333)], [(55, 555)]]),
    ("tuple", pa.struct([pa.field("a", pa.int64(), nullable=False)]), [{"a": 11}, {"a": 33}, {"a": 55}]),
):
    first, second, third = entries
    values = [first, None, second]
    write(f"{name}_unreferenced", value_type, [(values, [0, 2])])
    write(f"{name}_referenced", value_type, [(values, [0, 1, 2])])
    write(f"{name}_nullable", value_type, [(values, [0, None, 1, 2])], nullable=True)

    # Add a null entry to an initially all-valid dictionary, then append another all-valid delta.
    write(f"{name}_delta_added_unreferenced", value_type,
          [([first], [0]), (values, [2]), (values + [third], [3])], deltas=True)
    write(f"{name}_delta_added_referenced", value_type,
          [([first], [0]), (values, [1])], deltas=True)
    # A null entry in the base remains null after an all-valid delta; new entries remain valid.
    write(f"{name}_delta_base_unreferenced", value_type,
          [([first, None], [0]), (values, [2])], deltas=True)
    write(f"{name}_delta_base_referenced", value_type,
          [([first, None], [0]), (values, [1])], deltas=True)

    # Replacement dictionaries are supported by the stream format. Their null maps replace the base too.
    write(f"{name}_replacement_clears_null", value_type,
          [([first, None], [0]), ([second, third], [1])], formats=("ArrowStream",))
    write(f"{name}_replacement_adds_null", value_type,
          [([first, second], [0]), ([third, None], [1])], formats=("ArrowStream",))
PYEOF

write_query()
{
    echo "SELECT ${EXPR} FROM file('${TMP_DIR}/${TYPE}_$1.${FORMAT}', '${FORMAT}', '${CH_TYPE}'); ${2:-}"
}

# Expected-error annotations let all cases share a process while checking each rejection independently.
for FORMAT in Arrow ArrowStream; do
    TYPE=null
    CH_TYPE='c Nullable(Int64)'
    EXPR='c'
    echo "SELECT '${FORMAT} null: nullable field, non-nullable field';"
    write_query nullable
    write_query referenced "-- { serverError INCORRECT_DATA }"

    for TYPE in array map tuple; do
        case "$TYPE" in
            array) CH_TYPE='c Array(Int64)'; EXPR='sum(arraySum(c))' ;;
            map) CH_TYPE='c Map(Int64, Int64)'; EXPR='sum(arraySum(mapValues(c)))' ;;
            tuple) CH_TYPE='c Tuple(a Int64)'; EXPR='sum(c.a)' ;;
        esac
        echo "SELECT '${FORMAT} ${TYPE}: unreferenced null, nullable field, appended null, base null';"
        write_query unreferenced
        write_query nullable
        write_query delta_added_unreferenced
        write_query delta_base_unreferenced
        echo "SELECT '${FORMAT} ${TYPE}: referenced null, appended null, base null';"
        for CASE in referenced delta_added_referenced delta_base_referenced; do
            write_query "$CASE" "-- { serverError INCORRECT_DATA }"
        done
        if [[ "$FORMAT" == ArrowStream ]]; then
            echo "SELECT '${FORMAT} ${TYPE}: replacement clears null, replacement adds null';"
            write_query replacement_clears_null
            write_query replacement_adds_null "-- { serverError INCORRECT_DATA }"
        fi
    done
done > "$TMP_DIR/queries.sql"

${CLICKHOUSE_LOCAL} --path "$TMP_DIR/local" --max_threads=1 --allow_experimental_nullable_tuple_type=0 \
    --multiquery --queries-file "$TMP_DIR/queries.sql"
