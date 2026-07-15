#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Arrow format is not available in fasttest builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Arrow spec leaves the value bytes of unobservable slots undefined: slots that are null at any
# ancestor level (a struct's nulls do not have to be repeated in its children's validity), slot
# ranges no list offset references, and union child slots not selected by their row's type id. These
# files (generated with pyarrow) hide invalid values — an out-of-range date32 day number or an
# out-of-bounds dictionary index — where only that composed visibility reveals they can never be
# observed. Each file pairs the hidden garbage with at least one valid, observable row.
#
# Read with both Arrow readers:
#   struct_date32_garbage_under_null:            struct<d: date32>, garbage in the child of the NULL row
#   struct_dict_garbage_index_under_null:        struct<d: dictionary>, out-of-bounds index under NULL
#   struct_dict_default_garbage_index_under_null: as above, but the child field is non-nullable and the
#                                                dictionary holds its default value at index 0 — the
#                                                library reader's no-remap index arm, the native
#                                                reader's default-valued fallback key
#   struct_struct_date32_garbage_under_null:     struct<inner: struct<d>>, two levels of composition
#   struct_list_date32_garbage_under_null:       struct<l: list<date32>>, the NULL row's valid list slot
#                                                spans a range holding the garbage
#   struct_large_list_date32_garbage_under_null: as above with large_list
#   struct_fixed_size_list_date32_garbage_under_null: as above with fixed_size_list
#   struct_map_date32_garbage_under_null:        struct<m: map<string, date32>>, garbage map value
#   struct_string_view_garbage_view_under_null:  struct<v: string_view>, the NULL row's 16-byte view
#                                                struct holds garbage (length -1); the child's own
#                                                validity marks the slot valid
#   struct_json_garbage_bytes_under_null:        struct<j: string> read as Nullable(Tuple(j JSON));
#                                                the NULL row's bytes are not valid JSON
#   struct_ipv6_binary_garbage_under_null:       struct<v: binary> read as Nullable(Tuple(v IPv6));
#                                                the NULL row's value is not 16 bytes, which must not
#                                                force the whole column down the text-parsed String
#                                                fallback (that would corrupt the visible rows too)
#   struct_int128_binary_garbage_under_null:     as above for Nullable(Tuple(n Int128))
#
# Read only with the native reader — a NULL list/map slot whose offsets span a non-empty range
# (spec-legal: offsets stay monotonic, the range's bytes are undefined). The Apache Arrow library
# reader flattens list children through ListArray::Flatten, which drops the ranges of null non-empty
# slots, so its offsets no longer match the child and it rejects such files with INCORRECT_DATA
# regardless of the values (a pre-existing limitation):
#   list_date32_garbage_in_null_slot_range
#   fixed_size_list_date32_garbage_in_null_slot_range
#   map_date32_garbage_in_null_slot_range
#
# Also native-only (the library reader does not support Arrow unions at all). For unions the
# undefined bytes come from two more sources besides validity: child slots not selected by their
# row's type id (sparse layout, no nulls involved at all), and — under a NULL ancestor row — the
# union's own type-id/offset bytes:
#   sparse_union_date32_garbage_in_unselected:       non-selected sparse slots hold garbage
#   sparse_union_dict_garbage_index_in_unselected:   a dictionary child's non-selected slot holds an
#                                                    out-of-bounds index
#   struct_sparse_union_date32_garbage_under_null:   selected slot's garbage hidden by a NULL struct row
#   struct_dense_union_date32_garbage_under_null:    same for the dense layout (offsets-remapped)
#   struct_union_garbage_type_id_under_null:         the type-id byte itself is garbage (99) under NULL
#   struct_dense_union_garbage_offset_under_null:    the dense offset itself is garbage (999) under NULL

BOTH_READER_FILES="struct_date32_garbage_under_null \
    struct_dict_garbage_index_under_null \
    struct_dict_default_garbage_index_under_null \
    struct_struct_date32_garbage_under_null \
    struct_list_date32_garbage_under_null \
    struct_large_list_date32_garbage_under_null \
    struct_fixed_size_list_date32_garbage_under_null \
    struct_map_date32_garbage_under_null"

for reader in 1 0; do
    for f in $BOTH_READER_FILES; do
        echo "=== $f, native_reader=$reader"
        $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/$f.arrow', 'Arrow')
            SETTINGS allow_experimental_nullable_tuple_type = 1, input_format_arrow_use_native_reader = $reader"
    done

    echo "=== struct_string_view_garbage_view_under_null, native_reader=$reader"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_string_view_garbage_view_under_null.arrow', 'Arrow')
        SETTINGS allow_experimental_nullable_tuple_type = 1, input_format_arrow_use_native_reader = $reader"
    echo "=== struct_json_garbage_bytes_under_null, native_reader=$reader"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_json_garbage_bytes_under_null.arrow', 'Arrow', 's Nullable(Tuple(j JSON))')
        SETTINGS allow_experimental_nullable_tuple_type = 1, enable_json_type = 1, input_format_arrow_use_native_reader = $reader"
    echo "=== struct_ipv6_binary_garbage_under_null, native_reader=$reader"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_ipv6_binary_garbage_under_null.arrow', 'Arrow', 's Nullable(Tuple(v IPv6))')
        SETTINGS allow_experimental_nullable_tuple_type = 1, input_format_arrow_use_native_reader = $reader"
    echo "=== struct_int128_binary_garbage_under_null, native_reader=$reader"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/struct_int128_binary_garbage_under_null.arrow', 'Arrow', 's Nullable(Tuple(n Int128))')
        SETTINGS allow_experimental_nullable_tuple_type = 1, input_format_arrow_use_native_reader = $reader"
done

for f in list_date32_garbage_in_null_slot_range fixed_size_list_date32_garbage_in_null_slot_range map_date32_garbage_in_null_slot_range; do
    echo "=== $f, native reader"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/$f.arrow', 'Arrow')
        SETTINGS input_format_arrow_use_native_reader = 1"
done

for f in sparse_union_date32_garbage_in_unselected sparse_union_dict_garbage_index_in_unselected struct_sparse_union_date32_garbage_under_null struct_dense_union_date32_garbage_under_null struct_union_garbage_type_id_under_null struct_dense_union_garbage_offset_under_null; do
    echo "=== $f, native reader"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CUR_DIR/data_arrow/$f.arrow', 'Arrow')
        SETTINGS allow_experimental_nullable_tuple_type = 1, input_format_arrow_use_native_reader = 1"
done
