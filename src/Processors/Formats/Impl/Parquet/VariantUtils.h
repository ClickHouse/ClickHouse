#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <optional>

namespace DB
{
class DataTypeObject;
struct FormatSettings;
}

namespace DB::Parquet::VariantReader
{
struct VariantValue;
}

namespace DB::Parquet
{

inline constexpr std::string_view CLICKHOUSE_VARIANT_TYPE_HINTS_METADATA_KEY = "ClickHouse.variant_type_hints";
inline constexpr std::string_view CLICKHOUSE_VARIANT_WRAPPER_PATHS_METADATA_KEY = "ClickHouse.variant_wrapper_paths";

bool isNullableStringType(const IDataType * type);
bool isStoredJSONVariantTypeHint(std::string_view hint);

DataTypePtr getVariantTypeHintFromMetadata(std::string_view hint, bool enable_json_parsing);
String getVariantTypeHintForMetadata(const IDataType * type);

DataTypePtr unwrapVariantTypeHint(DataTypePtr type);
DataTypePtr makeVariantWrappedTypedValueType(const DataTypePtr & type);
DataTypePtr makeVariantExactOutputTypeNullable(const DataTypePtr & type);
DataTypePtr getParquetVariantScalarType(VariantPrimitiveType primitive_type, std::optional<UInt8> scale = {});
DataTypePtr inferVariantMaterializationType(const VariantReader::VariantValue & value, const FormatSettings & format_settings, size_t depth = 1);

String appendVariantJSONPath(std::string_view parent_path, std::string_view key);
String appendVariantMetadataPath(std::string_view parent_path, std::string_view key);
bool tryBuildNestedObjectField(
    Field flat_object_field,
    Field & nested_object_field,
    const FormatSettings & format_settings,
    size_t depth = 1);

struct VariantWrapperLayout
{
    size_t value_pos = 0;
    bool value_can_be_null = false;
    std::optional<size_t> typed_value_pos;
    DataTypePtr typed_value_type;
};

std::optional<VariantWrapperLayout> tryGetVariantWrapperLayout(const DataTypePtr & type);

bool isObjectLikeVariantOutputType(const IDataType * type);
bool isDynamicLikeVariantOutputType(const IDataType * type);
bool isComplexVariantExactOutputType(const DataTypePtr & type);

}
