#pragma once

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Core/Defines.h>
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

struct VariantSettings
{
    VariantSettings() = default;
    explicit VariantSettings(const FormatSettings & format_settings);

    size_t max_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH;
    bool escape_dots_in_json_keys = false;
    bool use_partial_match_to_skip_paths_by_regexp = true;
};

/// Decodes variant values, given as their `metadata` and `value` blobs, into a column of `output_type`,
/// which is `Dynamic`, `JSON` or `Nullable(JSON)`.
class VariantValueDecoder
{
public:
    VariantValueDecoder(DataTypePtr output_type_, String column_name_, const VariantSettings & settings_);

    /// Both blobs must hold an encoded value; a variant null is encoded too.
    void decode(std::string_view metadata_blob, std::string_view value_blob, IColumn & output) const;

private:
    DataTypePtr output_type;
    String column_name;
    VariantSettings settings;
    /// Set for `JSON`.
    std::shared_ptr<const JSONPathsFilter> filter;
};

class VariantValueEncoder
{
public:
    VariantValueEncoder(String column_name_, const VariantSettings & settings_);
    ~VariantValueEncoder();

    void encode(const IColumn & column, const IDataType & type, size_t row, String & metadata, String & value);

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

/// `output` must be a column of `output_type`, which is either `Dynamic` or `JSON`.
void decodeVariantColumn(
    const IColumn & metadata,
    const IColumn & value,
    IColumn & output,
    const DataTypePtr & output_type,
    const String & column_name,
    size_t num_rows,
    const VariantSettings & settings);

/// Encodes every row of `column` as an unshredded variant. With `null_map`, a NULL row of a `Dynamic` or
/// `Variant` column is marked there and gets empty blobs, otherwise it is a variant null.
void encodeVariantColumn(
    const IColumn & column,
    const DataTypePtr & type,
    const String & column_name,
    const VariantSettings & settings,
    ColumnString & metadata,
    ColumnString & value,
    PaddedPODArray<UInt8> * null_map);

}
