#!/usr/bin/env bash
# Tags: long, no-fasttest, no-asan, no-msan, no-tsan, no-debug

# Load various .parquet files from the internet, and files used by other tests.

# userdata{1..5}.parquet are from:
# wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata1.parquet
# ...
# wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata5.parquet

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_parquet

# TODO [parquet]: Known issues to investigate:

# ClickHouse Parquet reader doesn't support such complex types, so I didn't burrow into the issue.
# There is failure due parsing nested arrays or nested maps with NULLs:
# ../contrib/arrow/cpp/src/arrow/array/array_nested.cc:192:  Check failed: (self->list_type_->value_type()->id()) == (data->child_data[0]->type->id())

# Strange behaviour for repeated_no_annotation.parquet around __buitin_expect, so this file was disabled:
# debug:
#   ../contrib/arrow/cpp/src/arrow/array/array_nested.cc:193:  Check failed: self->list_type_->value_type()->Equals(data->child_data[0]->type)
# release:
#   Code: 349. DB::Ex---tion: Can not insert NULL data into non-nullable column "phoneNumbers": data for INSERT was parsed from stdin

EXCLUDE=(
    # These have extremely long strings and blow up the output.
    list_monotonically_increasing_offsets.parquet
    string_int_list_inconsistent_offset_multiple_batches.parquet
    # Date out of range.
    02716_data.parquet
    # GeoParquet files with Geometry (Variant) columns tested separately.
    03600_geoparquet_multi_geometry_empty_types.parquet
    03600_geoparquet_multi_geometry_explicit_types.parquet
    04614_geoparquet_multipoint_mixed.parquet
    04059_geo_spatial_pruning.parquet
    04060_geo_page_pruning.parquet
    04512_geo_pruning_iceberg.parquet
    04514_geo_pruning_iceberg_geostats.parquet
    04515_geo_pruning_iceberg_malformed_bbox.parquet
    04691_geo_page_pruning_null_bbox.parquet
    # Intentionally non-compliant files for testing DELTA_BINARY_PACKED padding tolerance.
    04045_delta_no_padding_3vals.parquet
    04045_delta_no_padding_5vals.parquet
    04045_delta_sample_93093.parquet
    # Hand-crafted file for testing dictionary memory estimation with sparse nullables.
    04099_dict_nullable_string_memory.parquet
    # Schema fixtures for the 04757 schema-inference cache key test. The VARIANT one carries a
    # logical type this reader rejects, so loading it here would print an exception.
    parquet_variant_logical_type.parquet
    parquet_required_json_column.parquet
    # Schema fixtures for 04065 Nullable(Tuple) wrapper test (LIST/MAP optional wrappers).
    04065_optional_list_wrapper_required_element.parquet
    04065_optional_map_wrapper_required_value.parquet
    04065_optional_struct_under_list.parquet
    04065_optional_struct_nullable_leaf_under_list.parquet
    # Hand-crafted file with an inconsistent bloom filter size for the 04654 out-of-bounds test.
    04654_bloom_filter_bitset_out_of_bounds.parquet
    # Hand-crafted DELTA_BYTE_ARRAY files for the 05035 malformed-input test. The first one is
    # malformed on purpose, so loading it here would print an exception.
    05035_delta_byte_array_zero_values.parquet
    05035_delta_byte_array_decimal.parquet
)

for NAME in $(find "$DATA_DIR" -type f \( -iname '*.parquet' -o -iname '*.parquet.gz' \) -print0 | xargs -0 -n 1 basename | LC_ALL=C sort | grep -vFf <(printf '%s\n' "${EXCLUDE[@]}")); do
    echo "=== Try load data from $NAME"

    # We want to read the file once and get both a hash of the whole data and a sample of a few
    # pseudorandomly chosen rows. We use a dummy GROUP BY WITH TOTALS for that.
    # (Maybe the unnecessary GROUP BY could be expensive if there are lots of rows, but these test
    #  files don't have lots of rows or they would be too big to check into git.)
    ${CLICKHOUSE_LOCAL} --query="
        SELECT _row_number, *, count(), sum(cityHash64(_row_number, *)) FROM file('$DATA_DIR/$NAME') GROUP BY all WITH TOTALS ORDER BY cityHash64(_row_number) LIMIT 20;" 2>&1
done
