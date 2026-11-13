#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <memory>
#include <string_view>

namespace DB
{
struct FormatSettings;
}

namespace DB::Parquet
{

class JSONPathsFilter;

/// Decodes variant values, given as their `metadata` and `value` blobs, into a column of `output_type`,
/// which is `Dynamic`, `JSON` or `Nullable(JSON)`.
class VariantValueDecoder
{
public:
    VariantValueDecoder(DataTypePtr output_type_, String column_name_, const FormatSettings & format_settings);

    /// Both blobs must hold an encoded value; a variant null is encoded too.
    void decode(std::string_view metadata_blob, std::string_view value_blob, IColumn & output) const;

private:
    DataTypePtr output_type;
    String column_name;
    size_t max_depth;
    /// Set for `JSON`.
    std::shared_ptr<const JSONPathsFilter> filter;
};

/// `output` must be a column of `output_type`, which is either `Dynamic` or `JSON`.
void decodeVariantColumn(
    const IColumn & metadata,
    const IColumn & value,
    IColumn & output,
    const DataTypePtr & output_type,
    const String & column_name,
    size_t num_rows,
    const FormatSettings & format_settings);

}
