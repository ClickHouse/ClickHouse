#pragma once

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>

#include <array>
#include <map>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
}

namespace DB::Parquet::VariantReader
{

inline void checkVariantReadDepth(const FormatSettings & format_settings, size_t depth)
{
    if (depth > format_settings.max_parser_depth)
    {
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Maximum parse depth ({}) exceeded while decoding `Parquet` `VARIANT`. Consider raising `max_parser_depth` setting.",
            format_settings.max_parser_depth);
    }
}

struct VariantMetadata
{
    std::vector<std::string_view> strings;
    bool strings_sorted = false;
};

VariantMetadata decodeMetadata(std::string_view metadata_blob, const FormatSettings & format_settings);

struct VariantMetadataCache
{
    const VariantMetadata & get(std::string_view metadata_blob, const FormatSettings & format_settings);

    std::unordered_map<String, VariantMetadata> cache;
};

struct ParsedVariantPathStep
{
    String remaining_path;
    String key;
};

struct ParsedVariantPath
{
    std::vector<ParsedVariantPathStep> steps;

    bool empty() const
    {
        return steps.empty();
    }
};

struct ResolvedVariantPathStep
{
    std::optional<UInt64> exact_match_field_id;
    std::optional<UInt64> nested_key_field_id;
};

struct ResolvedVariantPath
{
    ParsedVariantPath parsed_path;
    std::vector<ResolvedVariantPathStep> steps;
};

struct ScalarExactValue
{
    enum class PrimitiveKind
    {
        None,
        UInt8,
        Int8,
        Int16,
        Int32,
        Int64,
        Float32,
        Float64,
        Date32,
        DateTime64,
        Time64,
        Decimal32,
        Decimal64,
    };

    DataTypePtr exact_type;
    ColumnPtr exact_column;
    size_t exact_row_num = 0;
    std::optional<std::string_view> exact_string_view;
    PrimitiveKind primitive_kind = PrimitiveKind::None;
    UInt64 primitive_bits = 0;
};

enum class ScalarExactPathStatus
{
    Missing,
    Null,
    Exact,
    Unsupported,
};

struct VariantValue
{
    enum class Kind
    {
        Null,
        ExactValue,
        Array,
        Object,
        Tuple,
    };

    Kind kind = Kind::Null;
    DataTypePtr exact_type;
    ColumnPtr exact_column;
    size_t exact_row_num = 0;
    std::optional<std::string_view> exact_string_view;
    std::vector<VariantValue> array_elements;
    std::map<String, VariantValue> object_fields;
    std::vector<std::pair<String, VariantValue>> tuple_elements;

    bool isNull() const { return kind == Kind::Null; }
    bool isExactValue() const { return kind == Kind::ExactValue; }
    bool isArray() const { return kind == Kind::Array; }
    bool isObject() const { return kind == Kind::Object; }
    bool isTuple() const { return kind == Kind::Tuple; }
    bool isObjectLike() const { return isObject() || isTuple(); }
};

struct DecodedVariantValue
{
    VariantValue value;
};

struct MetadataState
{
    bool metadata_is_shared_across_rows = true;

    ColumnPtr metadata_column_holder;
    std::vector<std::optional<std::string_view>> metadata_values;
    std::optional<VariantMetadata> shared_metadata_storage;
    VariantMetadataCache metadata_cache;
    std::vector<const VariantMetadata *> metadata_by_row;
};

struct ConvertedTypedValue
{
    bool present = false;
    bool is_empty_container = false;
    VariantValue value;
};

struct SourceState
{
    struct PerTypedOutputState
    {
        std::array<bool, 2> typed_value_rows_prepared {false, false};
        std::array<std::vector<ConvertedTypedValue>, 2> typed_value_rows_by_preserve;
    };

    std::shared_ptr<MetadataState> metadata_state;
    bool value_column_is_all_null = true;
    ColumnPtr value_column_holder;
    std::vector<std::optional<std::string_view>> value_values;

    std::unordered_map<size_t, PerTypedOutputState> per_typed_output_state;
};

DecodedVariantValue decodeValue(const VariantMetadata & metadata, std::string_view value_blob, const FormatSettings & format_settings);
ParsedVariantPath parseVariantPath(std::string_view path);
ResolvedVariantPath resolveVariantPath(const VariantMetadata & metadata, const ParsedVariantPath & path);
bool tryDecodeValueByPath(const VariantMetadata & metadata, std::string_view value_blob, std::string_view path, VariantValue & result, const FormatSettings & format_settings, size_t depth = 1);
bool tryDecodeValueByPath(const VariantMetadata & metadata, std::string_view value_blob, const ParsedVariantPath & path, VariantValue & result, const FormatSettings & format_settings, size_t depth = 1);
bool tryDecodeValueByPath(const VariantMetadata & metadata, std::string_view value_blob, const ResolvedVariantPath & path, VariantValue & result, const FormatSettings & format_settings, size_t depth = 1);
ScalarExactPathStatus tryDecodeScalarExactValueByPath(
    const VariantMetadata & metadata,
    std::string_view value_blob,
    const ResolvedVariantPath & path,
    ScalarExactValue & result,
    const FormatSettings & format_settings,
    size_t depth = 1);
ScalarExactPathStatus tryDecodeScalarExactValue(
    std::string_view value_blob,
    ScalarExactValue & result,
    const FormatSettings & format_settings,
    size_t depth = 1);
void collectObjectFieldSlicesByResolvedIds(
    const VariantMetadata & metadata,
    std::string_view value_blob,
    const std::vector<UInt64> & field_ids,
    std::vector<std::optional<std::string_view>> & result_slices,
    const FormatSettings & format_settings,
    size_t depth = 1);
const VariantValue * tryGetSubcolumnValue(const VariantValue & value, std::string_view path, const FormatSettings & format_settings, size_t depth = 1);
const VariantValue * tryGetSubcolumnValue(const VariantValue & value, const ParsedVariantPath & path, const FormatSettings & format_settings, size_t depth = 1);
std::optional<VariantValue> extractSubcolumnValue(const VariantValue & value, const ParsedVariantPath & path, const FormatSettings & format_settings, size_t depth = 1);
void fillVariantValueJSON(const VariantValue & value, String & out, const FormatSettings & format_settings);
VariantValue mergeValues(VariantValue base, const VariantValue & patch, const FormatSettings & format_settings);
MutableColumnPtr materializeExactValueColumn(const VariantValue & value, const FormatSettings & format_settings);
bool isTypedArrayDefaultFiller(const ConvertedTypedValue & typed_value);
VariantValue mergeResidualValueWithTypedValue(
    DecodedVariantValue residual_value,
    const ConvertedTypedValue & typed_value,
    const FormatSettings & format_settings);

std::vector<ConvertedTypedValue> convertTypedColumnRange(
    const IColumn & column,
    const DataTypePtr & type,
    const VariantMetadata * const * metadata_by_row,
    size_t num_rows,
    size_t row_offset,
    const FormatSettings & format_settings,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata,
    std::optional<std::string_view> field_name,
    size_t depth = 1);

void insertValueIntoOutput(
    IColumn & output_column,
    const DataTypePtr & output_type,
    const VariantValue & value,
    const FormatSettings & format_settings);

void filterMetadataState(MetadataState & state, const IColumnFilter & filter, size_t result_size_hint, const FormatSettings & format_settings);
void filterSourceState(SourceState & state, const IColumnFilter & filter, size_t result_size_hint, const FormatSettings & format_settings);

}
