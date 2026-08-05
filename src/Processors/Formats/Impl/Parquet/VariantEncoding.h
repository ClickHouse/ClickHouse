#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace DB::Parquet
{

/// Encode one row of a column into the Parquet Variant binary encoding
/// (https://github.com/apache/parquet-format VariantEncoding.md).
/// `out_metadata` receives the encoded metadata (dictionary of field names), `out_value` the
/// encoded value. Named Tuples and Maps encode as objects, Arrays and unnamed Tuples as arrays,
/// scalars as the corresponding variant primitives. Dynamic values encode as their underlying
/// per-row type (preserving tuple field names nested in containers).
void encodeVariantValue(const IColumn & column, const DataTypePtr & type, size_t row, std::string & out_metadata, std::string & out_value);

/// A (column, type, row) triple identifying one value; see encodeVariantValue.
struct VariantRowRef
{
    const IColumn * column;
    DataTypePtr type;
    size_t row;
};

/// Resolve Nullable/LowCardinality/Dynamic wrappers of one row to the concrete value location
/// (nullptr when the row is null). Throws NOT_IMPLEMENTED for shared-variant Dynamic rows.
std::optional<VariantRowRef> resolveVariantRow(const IColumn & column, const DataTypePtr & type, size_t row);

/// Encode a value located at (column, type, row) with a prebuilt field-name dictionary
/// (see encodeVariantValue).
void encodeVariantValueWithDict(const VariantRowRef & value, const std::vector<String> & dict, std::string & out_value);

/// Encode the Variant metadata binary for a field-name dictionary (sorted lexicographically).
std::string encodeVariantMetadata(const std::vector<String> & dict);

/// Collect the sorted set of field names used by one row.
void collectVariantFieldNames(const IColumn & column, const DataTypePtr & type, size_t row, std::vector<String> & out_sorted_names);

/// Whether the type tree can contain a Map (whose keys are value-dependent and therefore not
/// cacheable by variant type).
bool variantTypeContainsMap(const IDataType & type);

}
