#include <Storages/StorageMaxCompute.h>
#include <Storages/MaxComputeReadSession.h>
#include <Storages/OdpsRecordReader.h>

#if USE_ODPS_TUNNEL
#    include <odps_clickhouse_adapter.h>
#    include <Columns/ColumnArray.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>
#    include <Core/AccurateComparison.h>
#    include <Core/ExternalResultDescription.h>
#    include <Core/Field.h>
#    include <Core/NamesAndTypes.h>
#    include <Core/Types.h>
#    include <Core/Settings.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/ProcessList.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <IO/Operators.h>
#    include <IO/WriteBufferFromString.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Parsers/ASTLiteral.h>
#    include <Parsers/ASTSetQuery.h>
#    include <QueryPipeline/Pipe.h>
#    include <Processors/ISource.h>
#    include <Storages/AlterCommands.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/NamedCollectionsHelpers.h>
#    if USE_ODPS_TUNNEL && USE_ODPS_ARROW
#        include <Formats/FormatFactory.h>
#        include <Storages/StorageMaxComputeArrow.h>
#    endif
#    include <boost/algorithm/string.hpp>
#    include <Common/Exception.h>
#    include <Common/Stopwatch.h>
#    include <Common/RemoteHostFilter.h>
#    include <Common/assert_cast.h>
#    include <Common/logger_useful.h>
#    include <Poco/URI.h>
#    include <base/defines.h>
#    include <base/range.h>
#    include <algorithm>
#    include <initializer_list>
#    include <map>
#    include <set>
#    include <string_view>
#    include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNSUPPORTED_METHOD;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int UNKNOWN_SETTING;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_maxcompute_storage_engine;
    extern const SettingsUInt64 odps_parallel_distributed_insert_select_start;
    extern const SettingsUInt64 odps_parallel_distributed_insert_select_count;
    extern const SettingsString odps_download_id;
    extern const SettingsBool odps_read_compress;
    extern const SettingsBool odps_parallel_local_insert_select;
    extern const SettingsUInt64 maxcompute_columnar_max_batch_bytes;
    extern const SettingsString maxcompute_read_format;
    extern const SettingsUInt64 maxcompute_max_retries;
    extern const SettingsUInt64 maxcompute_retry_initial_backoff_ms;
    extern const SettingsUInt64 maxcompute_retry_max_backoff_ms;
    extern const SettingsUInt64 maxcompute_retry_max_elapsed_ms;
    extern const SettingsUInt64 maxcompute_connect_timeout_ms;
    extern const SettingsUInt64 maxcompute_request_timeout_ms;
}

namespace
{
    using ValueType = ExternalResultDescription::ValueType;
    using IODPSTableSchemaPtr = apsara::odps::sdk::IODPSTableSchemaPtr;
    using ODPSColumnType = apsara::odps::sdk::ODPSColumnType;
    using ODPSArray = apsara::odps::sdk::ODPSArray;
    using ODPSMap = apsara::odps::sdk::ODPSMap;
    using ODPSStruct = apsara::odps::sdk::ODPSStruct;

    bool hasMaxComputeSetting(std::string_view name)
    {
        return name == "maxcompute_read_format";
    }

    void validateMaxComputeSettingName(std::string_view name)
    {
        if (!hasMaxComputeSetting(name))
            throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown MaxCompute table setting '{}'", name);
    }

    void validateMaxComputeReadFormat(const String & format, bool allow_inherit)
    {
        if (format != "row" && format != "column" && !(allow_inherit && format == "inherit"))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown `maxcompute_read_format` value '{}'; expected {}",
                format,
                allow_inherit ? "'inherit', 'row', or 'column'" : "'row' or 'column'");
    }

    String getMaxComputeTableReadFormat(const ASTPtr & settings_changes)
    {
        String format = "column";
        if (settings_changes)
        {
            for (const auto & change : settings_changes->as<ASTSetQuery &>().changes)
            {
                validateMaxComputeSettingName(change.name);
                format = change.value.safeGet<String>();
                validateMaxComputeReadFormat(format, false);
            }
        }
        return format;
    }

    void validateMaxComputeEndpoint(const String & endpoint, const RemoteHostFilter & remote_host_filter)
    {
        Poco::URI uri(endpoint);
        const String scheme = uri.getScheme();
        if (!boost::iequals(scheme, "http") && !boost::iequals(scheme, "https"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxCompute endpoint must use HTTP or HTTPS, got scheme '{}'", scheme);
        if (!uri.getUserInfo().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxCompute endpoint must not contain user information");
        if (uri.getHost().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxCompute endpoint must contain a host");

        remote_host_filter.checkURL(uri);
    }

    void validateMaxComputeBaseEndpoint(const String & endpoint, const RemoteHostFilter & remote_host_filter)
    {
        validateMaxComputeEndpoint(endpoint, remote_host_filter);
        const Poco::URI uri(endpoint);
        if (!uri.getQuery().empty() || !uri.getFragment().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxCompute endpoint must not contain a query string or fragment");
    }

    void validateMaxComputeColumns(const ColumnsDescription & columns)
    {
        for (const auto & column : columns.getAll())
        {
            const TypeIndex type_index = column.type->getTypeId();
            if (type_index == TypeIndex::Array)
            {
                const auto & array_type = assert_cast<const DataTypeArray &>(*column.type);
                if (array_type.getNestedType()->getTypeId() != TypeIndex::Nullable)
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unsupported column type: {}", column.type->getName());
            }
            else if (type_index == TypeIndex::Tuple)
            {
                const auto & tuple_type = assert_cast<const DataTypeTuple &>(*column.type);
                for (const auto & element : tuple_type.getElements())
                {
                    if (element->getTypeId() != TypeIndex::Nullable)
                        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unsupported column type: {}", column.type->getName());
                }
            }
            else if (type_index == TypeIndex::Nullable)
            {
                const auto & nullable_type = assert_cast<const DataTypeNullable &>(*column.type);
                const TypeIndex nested_type_index = nullable_type.getNestedType()->getTypeId();
                if (nested_type_index == TypeIndex::Array || nested_type_index == TypeIndex::Tuple)
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unsupported column type: {}", column.type->getName());
            }
        }
    }

    DataTypePtr getRequiredNullableNestedType(const DataTypePtr & type, const String & column_name)
    {
        if (type->getTypeId() != TypeIndex::Nullable)
        {
            throw Exception(
                ErrorCodes::BAD_TYPE_OF_FIELD,
                "MaxCompute complex column '{}' requires nullable nested elements, got {}",
                column_name,
                type->getName());
        }
        return assert_cast<const DataTypeNullable &>(*type).getNestedType();
    }

    std::string odpsDataTypeName(ODPSColumnType type)
    {
        switch (type)
        {
            case ODPSColumnType::ODPS_UNKNOWN: return "ODPS_UNKNOWN";
            case ODPSColumnType::ODPS_BIGINT: return "ODPS_BIGINT";
            case ODPSColumnType::ODPS_DOUBLE: return "ODPS_DOUBLE";
            case ODPSColumnType::ODPS_BOOLEAN: return "ODPS_BOOLEAN";
            case ODPSColumnType::ODPS_DATETIME: return "ODPS_DATETIME";
            case ODPSColumnType::ODPS_STRING: return "ODPS_STRING";
            case ODPSColumnType::ODPS_DECIMAL: return "ODPS_DECIMAL";
            case ODPSColumnType::ODPS_TINYINT: return "ODPS_TINYINT";
            case ODPSColumnType::ODPS_SMALLINT: return "ODPS_SMALLINT";
            case ODPSColumnType::ODPS_INTEGER: return "ODPS_INTEGER";
            case ODPSColumnType::ODPS_CHAR: return "ODPS_CHAR";
            case ODPSColumnType::ODPS_VARCHAR: return "ODPS_VARCHAR";
            case ODPSColumnType::ODPS_BINARY: return "ODPS_BINARY";
            case ODPSColumnType::ODPS_DATE: return "ODPS_DATE";
            case ODPSColumnType::ODPS_TIMESTAMP: return "ODPS_TIMESTAMP";
            case ODPSColumnType::ODPS_FLOAT: return "ODPS_FLOAT";
            case ODPSColumnType::ODPS_INTERVAL_YEAR_MONTH: return "ODPS_INTERVAL_YEAR_MONTH";
            case ODPSColumnType::ODPS_INTERVAL_DAY_TIME: return "ODPS_INTERVAL_DAY_TIME";
            case ODPSColumnType::ODPS_ARRAY: return "ODPS_ARRAY";
            case ODPSColumnType::ODPS_MAP: return "ODPS_MAP";
            case ODPSColumnType::ODPS_STRUCT: return "ODPS_STRUCT";
            case ODPSColumnType::ODPS_JSON: return "ODPS_JSON";
            case ODPSColumnType::ODPS_TIMESTAMP_NTZ: return "ODPS_TIMESTAMP_NTZ";
        }

        return fmt::format("ODPS_UNKNOWN({})", static_cast<int>(type));
    }

    inline constexpr const char * getTypeName(TypeIndex idx)
    {
        switch (idx)
        {
            case TypeIndex::Nothing:    return "Nothing";
            case TypeIndex::UInt8:      return "UInt8";
            case TypeIndex::UInt16:     return "UInt16";
            case TypeIndex::UInt32:     return "UInt32";
            case TypeIndex::UInt64:     return "UInt64";
            case TypeIndex::UInt128:    return "UInt128";
            case TypeIndex::UInt256:    return "UInt256";
            case TypeIndex::Int8:       return "Int8";
            case TypeIndex::Int16:      return "Int16";
            case TypeIndex::Int32:      return "Int32";
            case TypeIndex::Int64:      return "Int64";
            case TypeIndex::Int128:     return "Int128";
            case TypeIndex::Int256:     return "Int256";
            case TypeIndex::BFloat16:   return "BFloat16";
            case TypeIndex::Float32:    return "Float32";
            case TypeIndex::Float64:    return "Float64";
            case TypeIndex::Date:       return "Date";
            case TypeIndex::Date32:     return "Date32";
            case TypeIndex::DateTime:   return "DateTime";
            case TypeIndex::DateTime64: return "DateTime64";
            case TypeIndex::Time:       return "Time";
            case TypeIndex::Time64:     return "Time64";
            case TypeIndex::String:     return "String";
            case TypeIndex::FixedString: return "FixedString";
            case TypeIndex::Enum8:      return "Enum8";
            case TypeIndex::Enum16:     return "Enum16";
            case TypeIndex::Decimal32:  return "Decimal32";
            case TypeIndex::Decimal64:  return "Decimal64";
            case TypeIndex::Decimal128: return "Decimal128";
            case TypeIndex::Decimal256: return "Decimal256";
            case TypeIndex::UUID:       return "UUID";
            case TypeIndex::Array:      return "Array";
            case TypeIndex::Tuple:      return "Tuple";
            case TypeIndex::QBit:       return "QBit";
            case TypeIndex::Set:        return "Set";
            case TypeIndex::Interval:   return "Interval";
            case TypeIndex::Nullable:   return "Nullable";
            case TypeIndex::Function:   return "Function";
            case TypeIndex::AggregateFunction: return "AggregateFunction";
            case TypeIndex::LowCardinality: return "LowCardinality";
            case TypeIndex::Map:        return "Map";
            case TypeIndex::Object:     return "Object";
            case TypeIndex::IPv4:       return "IPv4";
            case TypeIndex::IPv6:       return "IPv6";
            case TypeIndex::JSONPaths:  return "JSONPaths";
            case TypeIndex::Variant:    return "Variant";
            case TypeIndex::Dynamic:    return "Dynamic";
        }

        UNREACHABLE();
    }

    template <typename To, typename From>
    To checkedIntegerCast(From value, const char * target_type, const std::string & col_name)
    {
        To result{};
        if (!accurate::convertNumeric<From, To, false>(value, result))
        {
            throw Exception(
                ErrorCodes::CANNOT_CONVERT_TYPE,
                "MaxCompute integer value is outside the range of ClickHouse type {} at column {}",
                target_type,
                col_name);
        }
        return result;
    }

    UInt32 checkedDateTimeSeconds(Int64 milliseconds, const std::string & col_name)
    {
        if (milliseconds < 0)
        {
            throw Exception(
                ErrorCodes::CANNOT_CONVERT_TYPE,
                "MaxCompute datetime value before the Unix epoch cannot be represented as ClickHouse DateTime at column {}",
                col_name);
        }
        return checkedIntegerCast<UInt32>(milliseconds / 1000, "DateTime", col_name);
    }

    void requireOdpsType(
        ODPSColumnType actual,
        ODPSColumnType expected,
        const char * clickhouse_type,
        const std::string & col_name)
    {
        if (actual != expected)
        {
            throw Exception(
                ErrorCodes::CANNOT_CONVERT_TYPE,
                "Cannot cast MaxCompute data type {} to ClickHouse {} at column {}",
                odpsDataTypeName(actual),
                clickhouse_type,
                col_name);
        }
    }

    const char * getStringValueFromOdpsTypes(
        const uint32_t & idx, uint32_t & len, const apsara::odps::sdk::ODPSTableRecord & record,
        const ODPSColumnType odps_column_type, const std::string & col_name)
    {
        switch (odps_column_type)
        {
            case ODPSColumnType::ODPS_STRING:
                return record.GetStringValue(idx, len);
            case ODPSColumnType::ODPS_CHAR:
                return record.GetCharValue(idx, len);
            case ODPSColumnType::ODPS_VARCHAR:
                return record.GetVarcharValue(idx, len);
            case ODPSColumnType::ODPS_BINARY:
                return record.GetBinaryValue(idx, len);
            case ODPSColumnType::ODPS_DECIMAL:
                return record.GetDecimalValue(idx, len);
            case ODPSColumnType::ODPS_JSON:
                return apsara::odps::sdk::clickhouse::getJSONValue(record, idx, len);
            default:
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Cannot cast MaxCompute data type {} to ClickHouse String at column {}", odpsDataTypeName(odps_column_type), col_name);
        }
    }

    const std::string getStringValueFromOdpsTypes(
        const uint32_t & idx, const apsara::odps::sdk::ODPSArray & odps_rray,
        const ODPSColumnType odps_column_type, const std::string & col_name)
    {
        switch (odps_column_type)
        {
            case ODPSColumnType::ODPS_STRING:
                return odps_rray.GetString(idx);
            case ODPSColumnType::ODPS_CHAR:
                return odps_rray.GetChar(idx);
            case ODPSColumnType::ODPS_VARCHAR:
                return odps_rray.GetVarchar(idx);
            case ODPSColumnType::ODPS_BINARY:
                return odps_rray.GetBinary(idx);
            case ODPSColumnType::ODPS_DECIMAL:
                return odps_rray.GetDecimal(idx);
            default:
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Cannot cast MaxCompute data type {} to ClickHouse String at column {}", odpsDataTypeName(odps_column_type), col_name);
        }
    }

    const std::string getStringValueFromOdpsTypes(
        const uint32_t & idx, const apsara::odps::sdk::ODPSStruct & odps_struct,
        const ODPSColumnType odps_column_type, const std::string & col_name)
    {
        switch (odps_column_type)
        {
            case ODPSColumnType::ODPS_STRING:
                return odps_struct.GetString(idx);
            case ODPSColumnType::ODPS_CHAR:
                return odps_struct.GetChar(idx);
            case ODPSColumnType::ODPS_VARCHAR:
                return odps_struct.GetVarchar(idx);
            case ODPSColumnType::ODPS_BINARY:
                return odps_struct.GetBinary(idx);
            case ODPSColumnType::ODPS_DECIMAL:
                return odps_struct.GetDecimal(idx);
            default:
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Cannot cast MaxCompute data type {} to ClickHouse String at column {}", odpsDataTypeName(odps_column_type), col_name);
        }
    }

#define ARRAY_VALUECASE(odps_rray, result, ODPS_TYPE, TYPE) \
        { \
            for (uint32_t i = 0; i < odps_rray.Size(); i++) \
            { \
                if (odps_rray.IsNull(i)) \
                { \
                    result.push_back(Null()); \
                } \
                else \
                    result.push_back(TYPE(odps_rray.Get##ODPS_TYPE(i))); \
            } \
        }

    Array convertArrayDataToField(
        const DataTypePtr & type,
        const ODPSColumnType & odps_column_type,
        const apsara::odps::sdk::ODPSArray & odps_rray,
        const std::string & col_name)
    {
        Array result;

        TypeIndex type_index = type->getTypeId();

        switch (type_index)
        {
            case TypeIndex::Int8: {
                switch (odps_column_type)
                {
                    case ODPSColumnType::ODPS_BOOLEAN:
                        ARRAY_VALUECASE(odps_rray, result, Bool, Int8)
                        break;
                    case ODPSColumnType::ODPS_TINYINT:
                        ARRAY_VALUECASE(odps_rray, result, TinyInt, Int8)
                        break;
                    default:
                        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                            "Cannot cast MaxCompute data type {} to ClickHouse UINT8 at column {}", odpsDataTypeName(odps_column_type), col_name);
                }
                break;
            }
            case TypeIndex::Int16:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_SMALLINT, "Int16", col_name);
                ARRAY_VALUECASE(odps_rray, result, SmallInt, Int16)
                break;
            case TypeIndex::Int32:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_INTEGER, "Int32", col_name);
                ARRAY_VALUECASE(odps_rray, result, Integer, Int32)
                break;
            case TypeIndex::Int64:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_BIGINT, "Int64", col_name);
                ARRAY_VALUECASE(odps_rray, result, BigInt, Int64)
                break;
            case TypeIndex::Float32:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_FLOAT, "Float32", col_name);
                ARRAY_VALUECASE(odps_rray, result, Float, Float32)
                break;
            case TypeIndex::Float64:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DOUBLE, "Float64", col_name);
                ARRAY_VALUECASE(odps_rray, result, Double, Float64)
                break;
            case TypeIndex::String: {
                for (uint32_t i = 0; i < odps_rray.Size(); i++)
                {
                    if (odps_rray.IsNull(i))
                    {
                        result.push_back(Null());
                    }
                    else
                        result.push_back(getStringValueFromOdpsTypes(i, odps_rray, odps_column_type, col_name));
                }
                break;
            }
            case TypeIndex::Date:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DATE, "Date", col_name);
                for (uint32_t i = 0; i < odps_rray.Size(); ++i)
                {
                    if (odps_rray.IsNull(i))
                        result.push_back(Null());
                    else
                        result.push_back(checkedIntegerCast<UInt16>(odps_rray.GetDateValue(i), "Date", col_name));
                }
                break;
            case TypeIndex::DateTime:
                {
                    requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DATETIME, "DateTime", col_name);
                    for (uint32_t i = 0; i < odps_rray.Size(); i++)
                    {
                        if (odps_rray.IsNull(i))
                        {
                            result.push_back(Null());
                        }
                        else
                            result.push_back(checkedDateTimeSeconds(odps_rray.GetDatetimeValue(i), col_name));
                    }
                }
                break;
            default: {
                std::string type_name = getTypeName(type_index);
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                    "Unsupported Array nested dataType yet: {} at column {}", type_name, col_name);
            }
        }

        return result;
    }

    Field convertStructDataToField(
        const DataTypePtr & type,
        const ODPSColumnType & odps_column_type,
        uint32_t struct_idx,
        const apsara::odps::sdk::ODPSStruct & odps_struct,
        const std::string & col_name)
    {
        Field result = Null();
        TypeIndex type_index = type->getTypeId();

        switch (type_index)
        {
            case TypeIndex::Int8: {
                switch (odps_column_type)
                {
                    case ODPSColumnType::ODPS_BOOLEAN:
                        result = Int8((odps_struct.GetBool(struct_idx)) ? 1 : 0);
                        break;
                    case ODPSColumnType::ODPS_TINYINT:
                        result = Int8(odps_struct.GetTinyInt(struct_idx));
                        break;
                    default:
                        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                            "Cannot cast MaxCompute data type {} to ClickHouse UINT8 at column {}", odpsDataTypeName(odps_column_type), col_name);
                }
                break;
            }
            case TypeIndex::Int16:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_SMALLINT, "Int16", col_name);
                result = Int16(odps_struct.GetSmallInt(struct_idx));
                break;
            case TypeIndex::Int32:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_INTEGER, "Int32", col_name);
                result = Int32(odps_struct.GetInteger(struct_idx));
                break;
            case TypeIndex::Int64:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_BIGINT, "Int64", col_name);
                result = Int64(odps_struct.GetBigInt(struct_idx));
                break;
            case TypeIndex::Float32:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_FLOAT, "Float32", col_name);
                result = Float32(odps_struct.GetFloat(struct_idx));
                break;
            case TypeIndex::Float64:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DOUBLE, "Float64", col_name);
                result = Float64(odps_struct.GetDouble(struct_idx));
                break;
            case TypeIndex::String: {
                result = getStringValueFromOdpsTypes(struct_idx, odps_struct, odps_column_type, col_name);
                break;
            }
            case TypeIndex::Date:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DATE, "Date", col_name);
                result = checkedIntegerCast<UInt16>(odps_struct.GetDateValue(struct_idx), "Date", col_name);
                break;
            case TypeIndex::DateTime:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DATETIME, "DateTime", col_name);
                result = checkedDateTimeSeconds(odps_struct.GetDatetimeValue(struct_idx), col_name);
                break;
            default:
                std::string type_name = getTypeName(type_index);
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                    "Unsupported Tuple nested dataType yet: {} at column {}", type_name, col_name);
        }

        return result;
    }

    Field convertNormalDataToField(
        const DataTypePtr & type,
        const apsara::odps::sdk::ODPSTableRecord & record,
        uint32_t idx,
        const ODPSColumnType & odps_column_type,
        const std::string & col_name)
    {
        Field result = Null();
        TypeIndex type_index = type->getTypeId();

        if (record.IsNullValue(idx))
        {
            if (type_index == TypeIndex::Nullable)
                return result;
            else
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "NonNullable column {} get Null value", col_name);
        }

        switch (type_index)
        {
            case TypeIndex::UInt8: {
                switch (odps_column_type)
                {
                    case ODPSColumnType::ODPS_BOOLEAN:
                        result = UInt8((*record.GetBoolValue(idx)) ? 1 : 0);
                        break;
                    case ODPSColumnType::ODPS_TINYINT:
                        result = checkedIntegerCast<UInt8>(*record.GetTinyIntValue(idx), "UInt8", col_name);
                        break;
                    default:
                        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                            "Cannot cast MaxCompute data type {} to ClickHouse UINT8 at column {}", odpsDataTypeName(odps_column_type), col_name);
                }
                break;
            }
            case TypeIndex::UInt16:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_SMALLINT, "UInt16", col_name);
                result = checkedIntegerCast<UInt16>(*record.GetSmallIntValue(idx), "UInt16", col_name);
                break;
            case TypeIndex::UInt32:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_INTEGER, "UInt32", col_name);
                result = checkedIntegerCast<UInt32>(*record.GetIntegerValue(idx), "UInt32", col_name);
                break;
            case TypeIndex::UInt64:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_BIGINT, "UInt64", col_name);
                result = checkedIntegerCast<UInt64>(*record.GetBigIntValue(idx), "UInt64", col_name);
                break;
            case TypeIndex::Int8:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_TINYINT, "Int8", col_name);
                result = Int8(*record.GetTinyIntValue(idx));
                break;
            case TypeIndex::Int16:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_SMALLINT, "Int16", col_name);
                result = Int16(*record.GetSmallIntValue(idx));
                break;
            case TypeIndex::Int32:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_INTEGER, "Int32", col_name);
                result = Int32(*record.GetIntegerValue(idx));
                break;
            case TypeIndex::Int64:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_BIGINT, "Int64", col_name);
                result = Int64(*record.GetBigIntValue(idx));
                break;
            case TypeIndex::Float32:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_FLOAT, "Float32", col_name);
                result = Float32(*record.GetFloatValue(idx));
                break;
            case TypeIndex::Float64:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DOUBLE, "Float64", col_name);
                result = Float64(*record.GetDoubleValue(idx));
                break;
            case TypeIndex::Decimal32:
            {
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DECIMAL, "Decimal32", col_name);
                try
                {
                    uint32_t len;
                    std::string rs = record.GetDecimalValue(idx, len);
                    const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal32> &>(*type);
                    result = DecimalField<Decimal32>(decimal_type.parseFromString(rs), decimal_type.getScale());
                    break;
                }
                catch (...)
                {
                    throw Exception(
                        ErrorCodes::BAD_TYPE_OF_FIELD,
                        "MaxCompute column {} cannot be converted to ClickHouse Decimal32: {}",
                        col_name,
                        getCurrentExceptionMessage(false));
                }
            }
            case TypeIndex::Decimal64:
            {
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DECIMAL, "Decimal64", col_name);
                try
                {
                    uint32_t len;
                    std::string rs = record.GetDecimalValue(idx, len);
                    const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal64> &>(*type);
                    result = DecimalField<Decimal64>(decimal_type.parseFromString(rs), decimal_type.getScale());
                    break;
                }
                catch (...)
                {
                    throw Exception(
                        ErrorCodes::BAD_TYPE_OF_FIELD,
                        "MaxCompute column {} cannot be converted to ClickHouse Decimal64: {}",
                        col_name,
                        getCurrentExceptionMessage(false));
                }
            }
            case TypeIndex::Decimal128:
            {
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DECIMAL, "Decimal128", col_name);
                try
                {
                    uint32_t len;
                    std::string rs = record.GetDecimalValue(idx, len);
                    const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal128> &>(*type);
                    result = DecimalField<Decimal128>(decimal_type.parseFromString(rs), decimal_type.getScale());
                    break;
                }
                catch (...)
                {
                    throw Exception(
                        ErrorCodes::BAD_TYPE_OF_FIELD,
                        "MaxCompute column {} cannot be converted to ClickHouse Decimal128: {}",
                        col_name,
                        getCurrentExceptionMessage(false));
                }
            }
            case TypeIndex::Decimal256:
            {
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DECIMAL, "Decimal256", col_name);
                try
                {
                    uint32_t len;
                    std::string rs = record.GetDecimalValue(idx, len);
                    const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal256> &>(*type);
                    result = DecimalField<Decimal256>(decimal_type.parseFromString(rs), decimal_type.getScale());
                    break;
                }
                catch (...)
                {
                    throw Exception(
                        ErrorCodes::BAD_TYPE_OF_FIELD,
                        "MaxCompute column {} cannot be converted to ClickHouse Decimal256: {}",
                        col_name,
                        getCurrentExceptionMessage(false));
                }
            }
            case TypeIndex::String: {
                uint32_t len;
                const char * ch = getStringValueFromOdpsTypes(idx, len, record, odps_column_type, col_name);
                std::string rs = ch ? std::string(ch, len) : std::string();
                result = rs;
                break;
            }
            case TypeIndex::Date:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DATE, "Date", col_name);
                result = checkedIntegerCast<UInt16>(*record.GetDateValue(idx), "Date", col_name);
                break;
            case TypeIndex::DateTime:
                requireOdpsType(odps_column_type, ODPSColumnType::ODPS_DATETIME, "DateTime", col_name);
                result = checkedDateTimeSeconds(*record.GetDatetimeValue(idx), col_name);
                break;
            case TypeIndex::Nullable:
                result = convertNormalDataToField(
                    (assert_cast<const DataTypeNullable *>(type.get()))->getNestedType(), record, idx, odps_column_type, col_name);
                break;
            default:
                std::string type_name = getTypeName(type_index);
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unsupported dataType yet: {} at column {}", type_name, col_name);
        }

        return result;
    }

    /// Renders an ODPS `ARRAY` / `MAP` / `STRUCT` value as a string, for a column declared
    /// `String` over a complex ODPS type.
    ///
    /// The SDK used to do this itself, through `ODPSArray::ToString`, `ODPSMap::ToString` and
    /// `ODPSStruct::ToString`. Those were dropped in SDK 0.42, so the rendering lives here now.
    /// It reproduces the old output byte for byte, quirks included, so that such a column keeps
    /// returning exactly what it returned before the upgrade:
    ///
    ///  - `STRING`, `CHAR` and `VARCHAR` are wrapped in `"` with no escaping, so a value that
    ///    itself contains `"` produces something that is not valid JSON;
    ///  - `BINARY` and `DECIMAL` are emitted unquoted, `BINARY` as raw bytes;
    ///  - `TINYINT` preserves the old `int8_t` stream output for a character type,
    ///    so it renders as a character rather than as a number;
    ///  - `DOUBLE` and `FLOAT` retain the old stream precision of 6 significant digits;
    ///  - an empty container renders as the empty string, not as `[]` or `{}`.
    ///
    /// Changing any of that would change query results, so it is deliberately left as it was.
    std::string odpsArrayToString(const ODPSArray & odps_array);
    std::string odpsMapToString(const ODPSMap & odps_map);
    std::string odpsStructToString(const ODPSStruct & odps_struct);

    /// `container` is an `ODPSArray` or an `ODPSStruct`; both expose the same element getters.
    template <typename Container>
    std::string odpsElementToString(const Container & container, ODPSColumnType odps_column_type, uint32_t idx)
    {
        if (container.IsNull(idx))
            return "null";

        WriteBufferFromOwnString out;
        switch (odps_column_type)
        {
            case ODPSColumnType::ODPS_BIGINT:
                out << container.GetBigInt(idx);
                break;
            case ODPSColumnType::ODPS_DOUBLE:
                out << fmt::format("{:.6g}", container.GetDouble(idx));
                break;
            case ODPSColumnType::ODPS_TINYINT:
                writeChar(static_cast<char>(container.GetTinyInt(idx)), out);
                break;
            case ODPSColumnType::ODPS_BOOLEAN:
                out << (container.GetBool(idx) ? "true" : "false");
                break;
            case ODPSColumnType::ODPS_DATETIME:
                out << container.GetDatetime(idx);
                break;
            case ODPSColumnType::ODPS_STRING:
                out << "\"" << container.GetString(idx) << "\"";
                break;
            case ODPSColumnType::ODPS_DECIMAL:
                out << container.GetDecimal(idx);
                break;
            case ODPSColumnType::ODPS_SMALLINT:
                out << container.GetSmallInt(idx);
                break;
            case ODPSColumnType::ODPS_INTEGER:
                out << container.GetInteger(idx);
                break;
            case ODPSColumnType::ODPS_CHAR:
                out << "\"" << container.GetChar(idx) << "\"";
                break;
            case ODPSColumnType::ODPS_VARCHAR:
                out << "\"" << container.GetVarchar(idx) << "\"";
                break;
            case ODPSColumnType::ODPS_BINARY:
                out << container.GetBinary(idx);
                break;
            case ODPSColumnType::ODPS_DATE:
                out << container.GetDate(idx);
                break;
            case ODPSColumnType::ODPS_TIMESTAMP:
                out << container.GetTimestamp(idx).ToString();
                break;
            case ODPSColumnType::ODPS_FLOAT:
                out << fmt::format("{:.6g}", container.GetFloat(idx));
                break;
            case ODPSColumnType::ODPS_INTERVAL_YEAR_MONTH:
                out << container.GetIntervalYearMonthValue(idx);
                break;
            case ODPSColumnType::ODPS_INTERVAL_DAY_TIME:
                out << container.GetIntervalDayTimeValue(idx).ToString();
                break;
            case ODPSColumnType::ODPS_ARRAY:
                out << odpsArrayToString(*container.GetArray(idx));
                break;
            case ODPSColumnType::ODPS_MAP:
                out << odpsMapToString(*container.GetMap(idx));
                break;
            case ODPSColumnType::ODPS_STRUCT:
                out << odpsStructToString(*container.GetStruct(idx));
                break;
            case ODPSColumnType::ODPS_TIMESTAMP_NTZ:
                /// New in SDK 0.42, so there is no previous output to preserve. Rendered like
                /// `ODPS_TIMESTAMP`, which is what it is minus the time zone.
                out << container.GetTimestampNTZ(idx).ToString();
                break;
            default:
                /// `ODPS_JSON` lands here: it is new in SDK 0.42 and the SDK exposes no getter
                /// for it on `ODPSArray` or `ODPSStruct`, only on `ODPSTableRecord`. Anything
                /// else the SDK may add later lands here too, and yields an empty string, which
                /// is what the old SDK did for types its own switch did not cover.
                break;
        }
        return out.str();
    }

    std::string odpsArrayToString(const ODPSArray & odps_array)
    {
        if (odps_array.Size() < 1)
            return "";

        std::string result("[");
        for (int64_t i = 0; i < odps_array.Size(); ++i)
        {
            if (i != 0)
                result += ",";
            result += odpsElementToString(odps_array, odps_array.GetElementType(), static_cast<uint32_t>(i));
        }
        result += "]";
        return result;
    }

    std::string odpsMapToString(const ODPSMap & odps_map)
    {
        if (odps_map.Size() < 1)
            return "";

        const std::shared_ptr<ODPSArray> keys = odps_map.GetKeys();
        const std::shared_ptr<ODPSArray> values = odps_map.GetValues();

        std::string result("{");
        for (int64_t i = 0; i < keys->Size(); ++i)
        {
            if (i != 0)
                result += ",";
            const auto idx = static_cast<uint32_t>(i);
            result += odpsElementToString(*keys, keys->GetElementType(), idx);
            result += ":";
            result += odpsElementToString(*values, values->GetElementType(), idx);
        }
        result += "}";
        return result;
    }

    std::string odpsStructToString(const ODPSStruct & odps_struct)
    {
        if (odps_struct.Size() < 1)
            return "";

        std::string result("{");
        for (int64_t i = 0; i < odps_struct.Size(); ++i)
        {
            if (i != 0)
                result += ",";

            /// Members are walked by index rather than through `GetMembers`. `ODPSStruct::Size`
            /// is `mSubTypes.size()` and `GetMemberType(i)` indexes the same vector, so this
            /// covers every member exactly once, in the order the struct declares them.
            ///
            /// `GetMembers` is what the SDK's own removed `ToString` used, but its definition
            /// went away with `ToString` and only the declaration is left in the header, so
            /// calling it does not link. It would also have been the worse choice: it returns
            /// the keys of an `unordered_map`, so the member order was unspecified, and the
            /// `GetMemberIndex` lookup that went with it upper-cases its argument, which throws
            /// for any struct whose member names are not already upper-case.
            const auto & member = odps_struct.GetMemberType(static_cast<uint32_t>(i));
            result += "\"";
            result += member.mMemberName;
            result += "\":";
            result += odpsElementToString(odps_struct, member.mType, static_cast<uint32_t>(i));
        }
        result += "}";
        return result;
    }

    Field convertOdpsDataToField(
        const DataTypePtr & type,
        const apsara::odps::sdk::ODPSTableRecord & record,
        size_t odps_recod_index_,
        const ODPSColumnType & odps_column_type,
        const std::string & col_name,
        bool odps_map_is_key)
    {
        uint32_t odps_recod_index = static_cast<uint32_t>(odps_recod_index_);
        TypeIndex type_index = type->getTypeId();

        if (record.IsNullValue(odps_recod_index))
        {
            if (type_index == TypeIndex::Nullable)
                return Null();
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "NonNullable column {} got a NULL MaxCompute value", col_name);
        }

        if (type_index == TypeIndex::Nullable)
        {
            return convertOdpsDataToField(
                assert_cast<const DataTypeNullable *>(type.get())->getNestedType(),
                record,
                odps_recod_index,
                odps_column_type,
                col_name,
                odps_map_is_key);
        }

        switch (odps_column_type)
        {
            case ODPSColumnType::ODPS_STRUCT: {
                if (type_index == TypeIndex::String)
                    return odpsStructToString(*record.GetStructValue(odps_recod_index));
                if (type_index != TypeIndex::Tuple)
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "odps struct need clickhouse dataType Tuple or String.");
                Tuple result;
                DataTypes elements_type = (assert_cast<const DataTypeTuple *>(type.get()))->getElements();
                if (!record.IsNullValue(odps_recod_index))
                {
                    std::shared_ptr<ODPSStruct> odps_struct = record.GetStructValue(odps_recod_index);
                    for (const auto i_ : collections::range(0, elements_type.size()))
                    {
                        uint32_t i = static_cast<uint32_t>(i_);
                        if (odps_struct->IsNull(i))
                        {
                            result.push_back(Null());
                        }
                        else
                        {
                            result.push_back(convertStructDataToField(
                                getRequiredNullableNestedType(elements_type[i], col_name),
                                odps_struct->GetMemberType(i).mType,
                                i,
                                *odps_struct,
                                col_name));
                        }
                    }
                    return result;
                }
                else
                {
                    for (size_t i = 0; i < elements_type.size(); ++i)
                    {
                        result.push_back(Null());
                    }
                    return result;
                }
            }
            case ODPSColumnType::ODPS_ARRAY: {
                if (type_index == TypeIndex::String)
                    return odpsArrayToString(*record.GetArrayValue(odps_recod_index));
                if (type_index != TypeIndex::Array)
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "odps array need clickhouse dataType Array or String.");
                Array result;
                const DataTypePtr element_type = (assert_cast<const DataTypeArray *>(type.get()))->getNestedType();
                if (!record.IsNullValue(odps_recod_index))
                {
                    std::shared_ptr<ODPSArray> odps_array = record.GetArrayValue(odps_recod_index);
                    result = convertArrayDataToField(
                        getRequiredNullableNestedType(element_type, col_name),
                        odps_array->GetElementType(),
                        *odps_array,
                        col_name);
                }
                return result;
            }
            case ODPSColumnType::ODPS_MAP: {
                if (type_index == TypeIndex::String)
                    return odpsMapToString(*record.GetMapValue(odps_recod_index));
                if (type_index != TypeIndex::Array)
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "odps map need clickhouse dataType Nested or String.");
                Array result;
                if (!record.IsNullValue(odps_recod_index))
                {
                    const DataTypePtr element_type = (assert_cast<const DataTypeArray *>(type.get()))->getNestedType();
                    std::shared_ptr<ODPSMap> odps_map = record.GetMapValue(odps_recod_index);
                    if (odps_map_is_key)
                    {
                        result = convertArrayDataToField(
                            getRequiredNullableNestedType(element_type, col_name),
                            odps_map->GetKeyTypeInfo().mType,
                            *odps_map->GetKeys(),
                            col_name);
                    }
                    else
                    {
                        result = convertArrayDataToField(
                            getRequiredNullableNestedType(element_type, col_name),
                            odps_map->GetValueTypeInfo().mType,
                            *odps_map->GetValues(),
                            col_name);
                    }
                }
                return result;
            }
            default:
                return convertNormalDataToField(type, record, odps_recod_index, odps_column_type, col_name);
        }
    }
}

namespace
{
    class MaxComputeSource : public ISource
    {
    public:
        String getName() const override { return "MaxCompute"; }

        MaxComputeSource(
            MaxComputeReadSessionPtr session_,
            const String & logger_name_,
            UInt64 max_block_size_,
            UInt64 count_,
            UInt64 start_,
            const Block & sample_block_,
            const std::vector<std::string> & odps_read_cols_,
            const std::vector<size_t> & odps_cols_index_,
            const std::vector<bool> odps_map_is_key_index_,
            bool compress_,
            UInt64 max_retries_,
            UInt64 retry_initial_backoff_ms_,
            UInt64 retry_max_backoff_ms_,
            UInt64 retry_max_elapsed_ms_)
            : ISource(std::make_shared<const Block>(sample_block_.cloneEmpty()))
            , max_block_size(max_block_size_)
            , log(&Poco::Logger::get(logger_name_))
            , session(std::move(session_))
            , count(count_)
        {
            sample_block = sample_block_;
            odps_cols_index = odps_cols_index_;
            odps_map_is_key_index = odps_map_is_key_index_;

            if (count != 0)
            {
                reader = std::make_unique<AutoReconnectRecordReader>(
                    [read_session = this->session]
                    {
                        return read_session->createReaderDownload();
                    },
                    start_,
                    count,
                    odps_read_cols_,
                    compress_,
                    max_retries_,
                    retry_initial_backoff_ms_,
                    retry_max_backoff_ms_,
                    retry_max_elapsed_ms_,
                    [this]
                    {
                        return isCancelled();
                    },
                    log);
                odps_column_count = sample_block.columns();
                odps_table_record = reader->createBufferRecord();
                odps_table_schema.reset(odps_table_record->GetSchema()->Clone());
            }
            else
                finishTask();
        }

        ~MaxComputeSource() override
        {
            try
            {
                if (reader)
                    reader->close();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to close ODPS reader");
            }
        }

    protected:
        Chunk generate() override
        {
            if (count == 0 || eof)
                return {};

            MutableColumns columns(sample_block.columns());
            for (const auto i : collections::range(0, columns.size()))
                columns[i] = sample_block.getByPosition(i).column->cloneEmpty();

            size_t rows_count = 0;
            while (rows_count < max_block_size && reader->read(*odps_table_record))
            {
                for (const auto idx : collections::range(0, odps_column_count))
                {
                    (*columns[idx])
                        .insert(convertOdpsDataToField(
                            sample_block.getByPosition(idx).type,
                            *odps_table_record,
                            static_cast<uint32_t>(odps_cols_index[idx]),
                            odps_table_schema->GetTableColumn(static_cast<uint32_t>(odps_cols_index[idx])).GetType(),
                            sample_block.getByPosition(idx).name,
                            odps_map_is_key_index[idx]));
                }
                ++rows_count;
            }

            if (rows_count == 0)
            {
                eof = true;
                reader->close();
                finishTask();
                UInt64 total_time_ns = watch.elapsed();
                LOG_TRACE(log, "MaxCompute reader finished. Total cost: {}ms, read rows: {}",
                          total_time_ns / 1000000, reader->totalReadRows());
                return {};
            }

            return Chunk(std::move(columns), rows_count);
        }

    private:
        void finishTask()
        {
            if (task_finished)
                return;
            task_finished = true;
            session->finishTask(count, reader ? reader->totalReadRows() : 0);
        }

        UInt64 max_block_size;
        Poco::Logger * log;

        MaxComputeReadSessionPtr session;
        std::unique_ptr<AutoReconnectRecordReader> reader;
        bool eof = false;
        bool task_finished = false;
        apsara::odps::sdk::IODPSTableSchemaPtr odps_table_schema;
        size_t odps_column_count;
        apsara::odps::sdk::ODPSTableRecordPtr odps_table_record;
        Stopwatch watch;

        UInt64 count;
        Block sample_block;
        /// Used to locate struct Map key and Map value index in odps_table_record.
        std::vector<size_t> odps_cols_index;
        std::vector<bool> odps_map_is_key_index;
    };

}


StorageMaxCompute::StorageMaxCompute(
    const StorageID & table_id_,
    const std::string & odps_tunnel_endpoint_,
    const std::string & project_name_,
    const std::string & table_name_,
    const std::string & partition_spec_,
    const std::string & user_name_,
    const std::string & password_,
    const std::string & sts_token_,
    const uint64_t start_,
    const uint64_t count_,
    const uint64_t thread_num_,
    const std::string & odps_endpoint_,
    const std::string & quota_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const ASTPtr & settings_changes_)
    : IStorage(table_id_)
    , odps_tunnel_endpoint(odps_tunnel_endpoint_)
    , project_name(project_name_)
    , table_name(table_name_)
    , partition_spec(partition_spec_)
    , user_name(user_name_)
    , password(password_)
    , sts_token(sts_token_)
    , start(start_)
    , count(count_)
    , thread_num(thread_num_)
    , odps_endpoint(odps_endpoint_)
    , quota_name(quota_name_)
    , log(&Poco::Logger::get(table_id_.getNameForLogs()))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    getMaxComputeTableReadFormat(settings_changes_);
    if (settings_changes_)
        storage_metadata.setSettingsChanges(settings_changes_->clone());
    else
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_metadata.setSettingsChanges(settings_ast);
    }
    setInMemoryMetadata(storage_metadata);
}

void StorageMaxCompute::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    for (const auto & command : commands)
    {
        if (command.type == AlterCommand::MODIFY_SETTING || command.type == AlterCommand::RESET_SETTING)
        {
            for (const auto & change : command.settings_changes)
            {
                validateMaxComputeSettingName(change.name);
                validateMaxComputeReadFormat(change.value.safeGet<String>(), false);
            }
            for (const auto & name : command.settings_resets)
                validateMaxComputeSettingName(name);
        }
        else if (!command.isCommentAlter())
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
        }
    }
}

size_t StorageMaxCompute::getMaxReadStreams(size_t num_streams, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::odps_parallel_local_insert_select])
        return 1;

    if (thread_num > 1)
        return static_cast<size_t>(thread_num);

    return std::max<size_t>(1, num_streams);
}

Pipe StorageMaxCompute::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);
    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    const Settings & settings = context->getSettingsRef();
    String read_format = settings[Setting::maxcompute_read_format];
    validateMaxComputeReadFormat(read_format, true);
    if (read_format == "inherit")
        read_format = getMaxComputeTableReadFormat(storage_snapshot->metadata->getSettingsChanges());
#if !USE_ODPS_ARROW
    if (read_format == "column")
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MaxCompute column format requires Arrow support, which is not available in this build");
#endif
    const auto query_status = context->getProcessListElement();
    const String & endpoint_to_validate = odps_endpoint.empty() ? odps_tunnel_endpoint : odps_endpoint;
    validateMaxComputeBaseEndpoint(endpoint_to_validate, context->getRemoteHostFilter());

    uint64_t actual_start
        = (settings[Setting::odps_parallel_distributed_insert_select_start].value == 0
               ? start
               : settings[Setting::odps_parallel_distributed_insert_select_start].value);

    uint64_t actual_count = 0;
    std::string odps_tunnel_id = settings[Setting::odps_download_id].value;
    if (!odps_tunnel_id.empty() && context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The internal setting `odps_download_id` cannot be supplied by an initial query");

    auto session = std::make_shared<MaxComputeReadSession>(
        MaxComputeConnectionConfiguration{
            .tunnel_endpoint = odps_tunnel_endpoint,
            .odps_endpoint = odps_endpoint,
            .project = project_name,
            .table = table_name,
            .partition_spec = partition_spec,
            .access_key_id = user_name,
            .access_key_secret = password,
            .sts_token = sts_token,
            .quota_name = quota_name,
            .connect_timeout_ms = settings[Setting::maxcompute_connect_timeout_ms],
            .request_timeout_ms = settings[Setting::maxcompute_request_timeout_ms],
            .endpoint_validator = [context](const String & endpoint)
            {
                validateMaxComputeEndpoint(endpoint, context->getRemoteHostFilter());
            },
            .resolved_endpoint_observer = [logger = this->log](const String & endpoint)
            {
                if (boost::iequals(Poco::URI(endpoint).getScheme(), "http"))
                {
                    LOG_WARNING(
                        logger,
                        "MaxCompute resolved endpoint uses unencrypted HTTP; keep this only for legacy compatibility and migrate the table to HTTPS");
                }
            },
            .cancellation_checker = [query_status]
            {
                return query_status && query_status->isKilled();
            }},
        odps_tunnel_id);

    const uint64_t total_count = session->getRecordCount();
    if (actual_start > total_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Start index ({}) is larger than total record count ({})",
            actual_start,
            total_count);
    }

    if (settings[Setting::odps_parallel_distributed_insert_select_count].value == 0)
    {
        const uint64_t available_count = total_count - actual_start;
        if (count == 0)
            actual_count = available_count;
        else
            actual_count = std::min(count, available_count);
    }
    else
    {
        actual_count = settings[Setting::odps_parallel_distributed_insert_select_count].value;
        if (actual_count > total_count - actual_start)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Requested MaxCompute range starting at {} with count {} exceeds total record count {}",
                actual_start,
                actual_count,
                total_count);
        }
    }

    uint64_t actual_num_stream = settings[Setting::odps_parallel_local_insert_select]
        ? std::max<uint64_t>(1, thread_num > 1 ? thread_num : num_streams)
        : 1;

    if (actual_count == 0)
        actual_num_stream = 1;
    else
        actual_num_stream = std::min(actual_num_stream, actual_count);

    uint64_t step = actual_count / actual_num_stream;
    uint64_t last_step = step + (actual_count - step * actual_num_stream);

    session->initializeTasks(actual_num_stream, actual_count);

    LOG_INFO(log, "maxcompute total record: {}, num of streams: {} ", actual_count, actual_num_stream);

    std::vector<std::string> odps_read_cols;
    //init odps complex type(map) columns index
    std::vector<size_t> odps_cols_index;
    std::vector<bool> odps_map_is_key_index;

    std::vector<std::string> cols_prefix_name;
    std::vector<std::string> cols_suffix_name;
    std::set<std::string> odps_read_cols_set;
    for (const auto i : collections::range(0, sample_block.columns()))
    {
        std::string col_name = sample_block.getByPosition(i).name;
        std::vector<std::string> split_strings;
        boost::split(split_strings, col_name, boost::is_any_of("."), boost::token_compress_on);
        if (split_strings.size() > 1)
        {
            if (split_strings.size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported MaxCompute nested field path '{}'", col_name);
            if (odps_read_cols_set.find(split_strings[0]) == odps_read_cols_set.end())
            {
                odps_read_cols.push_back(split_strings[0]);
                odps_read_cols_set.insert(split_strings[0]);
            }
            cols_prefix_name.push_back(split_strings[0]);
            cols_suffix_name.push_back(split_strings[1]);
        }
        else
        {
            odps_read_cols_set.insert(col_name);
            cols_prefix_name.push_back(col_name);
            odps_read_cols.push_back(col_name);
            //placeholder is string '-'
            cols_suffix_name.push_back("-");
        }
    }
    std::map<std::string, size_t> odps_read_cols_index;
    for (const auto i : collections::range(0, odps_read_cols.size()))
    {
        odps_read_cols_index.insert(std::pair<std::string, size_t>(odps_read_cols[i], i));
    }
    for (const auto i : collections::range(0, cols_prefix_name.size()))
    {
        odps_cols_index.push_back(odps_read_cols_index.find(cols_prefix_name[i])->second);
    }

    /// Resolve MAP members by their explicit field path, never by table-column
    /// order. This also makes projecting only `.key` or only `.value` stable.
    const auto * odps_schema = session->getSchema();
    for (const auto i : collections::range(0, cols_suffix_name.size()))
    {
        const String & suffix = cols_suffix_name[i];
        if (suffix == "-")
        {
            odps_map_is_key_index.push_back(true);
            continue;
        }

        if (suffix != "key" && suffix != "value")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unsupported MaxCompute nested field path '{}.{}'; MAP fields must use `.key` or `.value`",
                cols_prefix_name[i],
                suffix);

        const apsara::odps::sdk::IODPSTableColumn * odps_column = nullptr;
        for (uint32_t column_index = 0; column_index < odps_schema->GetColumnCount(); ++column_index)
        {
            const auto & candidate = odps_schema->GetTableColumn(column_index);
            if (boost::iequals(candidate.GetName(), cols_prefix_name[i]))
            {
                odps_column = &candidate;
                break;
            }
        }
        if (!odps_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxCompute column '{}' does not exist", cols_prefix_name[i]);

        if (odps_column->GetType() != ODPSColumnType::ODPS_MAP)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "MaxCompute nested field '{}.{}' requires '{}' to be an ODPS MAP, got {}",
                cols_prefix_name[i],
                suffix,
                cols_prefix_name[i],
                odpsDataTypeName(odps_column->GetType()));
        }

        odps_map_is_key_index.push_back(suffix == "key");
    }

    bool compress = settings[Setting::odps_read_compress];
    const UInt64 max_retries = settings[Setting::maxcompute_max_retries];
    const UInt64 retry_initial_backoff_ms = settings[Setting::maxcompute_retry_initial_backoff_ms];
    const UInt64 retry_max_backoff_ms = settings[Setting::maxcompute_retry_max_backoff_ms];
    const UInt64 retry_max_elapsed_ms = settings[Setting::maxcompute_retry_max_elapsed_ms];
    if (retry_initial_backoff_ms > retry_max_backoff_ms)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`maxcompute_retry_initial_backoff_ms` cannot exceed `maxcompute_retry_max_backoff_ms`");

#if USE_ODPS_ARROW
    const bool use_columnar = read_format == "column";
    FormatSettings format_settings;
    if (use_columnar)
    {
        OdpsArrowColumnMatcher::validate(session->getSchema(), odps_read_cols, sample_block);
        format_settings = getFormatSettings(context);
    }
#endif

    Pipes pipes;
    for (size_t i = 0; i < actual_num_stream; ++i)
    {
#if USE_ODPS_ARROW
        /// Columnar shards share the row-based sharding arithmetic (step /
        /// last_step); the source pulls whole Arrow batches instead of
        /// records, so the MAP folding indices used by the row path do not
        /// apply here (name matching happens inside the converter).
        if (use_columnar)
        {
            pipes.emplace_back(std::make_shared<MaxComputeArrowSource>(
                session,
                project_name + "_" + table_name,
                max_block_size,
                i != actual_num_stream - 1 ? step : last_step,
                actual_start + i * step,
                sample_block,
                odps_read_cols,
                compress,
                settings[Setting::maxcompute_columnar_max_batch_bytes].value,
                max_retries,
                retry_initial_backoff_ms,
                retry_max_backoff_ms,
                retry_max_elapsed_ms,
                format_settings));
            continue;
        }
#endif

        if (i != actual_num_stream - 1)
        {
            pipes.emplace_back(std::make_shared<MaxComputeSource>(
                session,
                project_name + "_" + table_name,
                max_block_size,
                step,
                actual_start + i * step,
                sample_block,
                odps_read_cols,
                odps_cols_index,
                odps_map_is_key_index,
                compress,
                max_retries,
                retry_initial_backoff_ms,
                retry_max_backoff_ms,
                retry_max_elapsed_ms));
        }
        else
        {
            pipes.emplace_back(std::make_shared<MaxComputeSource>(
                session,
                project_name + "_" + table_name,
                max_block_size,
                last_step,
                actual_start + i * step,
                sample_block,
                odps_read_cols,
                odps_cols_index,
                odps_map_is_key_index,
                compress,
                max_retries,
                retry_initial_backoff_ms,
                retry_max_backoff_ms,
                retry_max_elapsed_ms));
        }
    }

    return Pipe::unitePipes(std::move(pipes));
}

/// Keep a prototype visible in this translation unit for `-Wmissing-prototypes`,
/// following the same local-declaration pattern as the other storage engines.
void registerStorageMaxCompute(StorageFactory & factory);
void registerStorageMaxCompute(StorageFactory & factory)
{
    auto createStorageMaxCompute = [](const StorageFactory::Arguments & args, const std::string & storage_engine_name)
    {
        if (isFreshTableDefinition(args.mode, args.query.attach_short_syntax)
            && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_maxcompute_storage_engine])
        {
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "MaxCompute table engines are experimental. Set `allow_experimental_maxcompute_storage_engine` to enable them");
        }

        ASTs & engine_args = args.engine_args;
        validateMaxComputeColumns(args.columns);
        if (auto named_collection = tryGetNamedCollectionWithOverrides(
                engine_args,
                args.getLocalContext(),
                true,
                nullptr,
                &args.table_id))
        {
            static const std::set<String> allowed_keys = {
                "endpoint",
                "endpoint_mode",
                "project",
                "table",
                "partition",
                "partition_spec",
                "access_key_id",
                "user",
                "username",
                "access_key_secret",
                "secret_access_key",
                "password",
                "sts_token",
                "session_token",
                "token",
                "quota_name",
                "thread_num",
                "start",
                "count",
            };
            for (const auto & key : named_collection->getKeys())
            {
                if (!allowed_keys.contains(key))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected key `{}` in MaxCompute named collection", key);
            }

            const auto rejectDuplicateAliases = [&](std::initializer_list<String> keys, const char * logical_name)
            {
                size_t count = 0;
                for (const auto & key : keys)
                    count += named_collection->has(key);
                if (count > 1)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "MaxCompute named collection specifies more than one alias for `{}`",
                        logical_name);
            };
            rejectDuplicateAliases({"partition", "partition_spec"}, "partition");
            rejectDuplicateAliases({"access_key_id", "username", "user"}, "access_key_id");
            rejectDuplicateAliases({"access_key_secret", "secret_access_key", "password"}, "access_key_secret");
            rejectDuplicateAliases({"sts_token", "session_token", "token"}, "sts_token");

            const String endpoint = named_collection->get<String>("endpoint");
            const String project = named_collection->get<String>("project");
            const String table = named_collection->get<String>("table");
            const String partition = named_collection->getAnyOrDefault<String>({"partition", "partition_spec"}, "");
            const String username = named_collection->getAny<String>({"access_key_id", "username", "user"});
            const String password = named_collection->getAny<String>({"access_key_secret", "secret_access_key", "password"});
            const String sts_token = named_collection->getAnyOrDefault<String>({"sts_token", "session_token", "token"}, "");
            const UInt64 thread_num = named_collection->getOrDefault<UInt64>("thread_num", 1);
            const String quota_name = named_collection->getOrDefault<String>("quota_name", "default");
            const UInt64 start = named_collection->getOrDefault<UInt64>("start", 0);
            const UInt64 count = named_collection->getOrDefault<UInt64>("count", 0);
            const String default_endpoint_mode = storage_engine_name == "MaxComputeRaw" ? "odps_router" : "tunnel";
            const String endpoint_mode = named_collection->getOrDefault<String>("endpoint_mode", default_endpoint_mode);
            if (endpoint_mode != "tunnel" && endpoint_mode != "odps_router")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown MaxCompute `endpoint_mode` '{}'", endpoint_mode);

            return std::make_shared<StorageMaxCompute>(
                args.table_id,
                endpoint,
                project,
                table,
                partition,
                username,
                password,
                sts_token,
                start,
                count,
                thread_num,
                endpoint_mode == "odps_router" ? endpoint : "",
                quota_name,
                args.columns,
                args.constraints,
                args.storage_def->settings ? args.storage_def->settings->clone() : nullptr);
        }

        if (engine_args.size() < 6 || engine_args.size() > 10)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage MaxCompute requires 6-10 parameters: MaxCompute('tunnel_endpoint', project, table, 'partition', 'user', "
                "'password'[, thread_num, 'quota_name', start, count]).");

        for (size_t i = 0; i < engine_args.size(); ++i)
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.getLocalContext());

        const String & odps_tunnel_endpoint = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        const String & project = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & partition = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        const String & username = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
        const String & password = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();
        uint64_t thread_num = 1;
        if (engine_args.size() >= 7)
            thread_num = engine_args[6]->as<ASTLiteral &>().value.safeGet<UInt64>();

        /// Name of the ODPS Tunnel quota to route reads through, for bandwidth isolation between
        /// workloads. `default` is the implicit quota of a project. An empty string makes the SDK
        /// leave the `quotaName` request parameter out altogether.
        String quota_name = "default";
        bool legacy_numeric_eighth_argument = false;
        if (engine_args.size() >= 8)
        {
            const Field & quota_name_arg = engine_args[7]->as<ASTLiteral &>().value;
            if (quota_name_arg.getType() == Field::Types::String)
                quota_name = quota_name_arg.safeGet<String>();
            else
                legacy_numeric_eighth_argument = true;
        }

        uint64_t start = 0;
        uint64_t count = 0;
        if (legacy_numeric_eighth_argument)
        {
            if (engine_args.size() > 9)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Storage {} with a numeric 8th argument accepts only legacy `start` and optional `count` arguments",
                    storage_engine_name);
            start = engine_args[7]->as<ASTLiteral &>().value.safeGet<UInt64>();
            if (engine_args.size() == 9)
                count = engine_args[8]->as<ASTLiteral &>().value.safeGet<UInt64>();
        }
        else if (engine_args.size() >= 9)
        {
            start = engine_args[8]->as<ASTLiteral &>().value.safeGet<UInt64>();
            if (engine_args.size() == 10)
                count = engine_args[9]->as<ASTLiteral &>().value.safeGet<UInt64>();
        }

        String odps_endpoint = "";
        if (storage_engine_name == "MaxComputeRaw")
            odps_endpoint = odps_tunnel_endpoint;

        return std::make_shared<StorageMaxCompute>(
            args.table_id,
            odps_tunnel_endpoint,
            project,
            table,
            partition,
            username,
            password,
            "",
            start,
            count,
            thread_num,
            odps_endpoint,
            quota_name,
            args.columns,
            args.constraints,
            args.storage_def->settings ? args.storage_def->settings->clone() : nullptr);
    };

    const auto description = R"DOCS_MD(
The `MaxCompute` and `MaxComputeRaw` table engines read existing Alibaba Cloud MaxCompute (ODPS) tables through the Tunnel API.
They support `SELECT` and importing data into ClickHouse with `INSERT INTO clickhouse_table SELECT ... FROM maxcompute_table`.
Writing to MaxCompute, automatic schema inference, and the MaxCompute Storage API are not supported.

The engines require a ClickHouse build with `ENABLE_ODPS_TUNNEL`. Arrow reads additionally require `ENABLE_ODPS_ARROW` and ClickHouse's Arrow dependency.

## Enabling the experimental engines {#enabling-the-experimental-engines}

Both engines are experimental and disabled for new table creation by default. Enable them in the session before creating a table:

```sql
SET allow_experimental_maxcompute_storage_engine = 1;
```

`allow_experimental_maxcompute_storage_engine` is a query/session setting that can also be enabled in a settings profile. It is not a table setting: do not put it in the table's `CREATE TABLE ... SETTINGS` clause.

The gate applies to both engine names, positional arguments and named collections, including full `ATTACH TABLE` definitions supplied by a user. It does not prevent loading existing metadata at startup or using short-form `ATTACH TABLE table_name`. Reading existing tables and changing their read format do not require this setting to remain enabled. The gate does not replace access-control checks.

The examples below assume this setting has been enabled in the session.

## Creating a table {#creating-a-table}

Declare the columns explicitly. Column names must match the remote table; a subset of the remote columns can be declared.

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 type1,
    name2 type2,
    ...
)
ENGINE = MaxCompute(
    'tunnel_endpoint', 'project', 'table', 'partition',
    'access_key_id', 'access_key_secret'
    [, thread_num [, 'quota_name' [, start [, count]]]]
);
```

`MaxComputeRaw` accepts the same arguments, but its first argument is an ODPS service endpoint. It resolves the Tunnel endpoint through the ODPS router before opening the download session. `MaxCompute` uses the supplied Tunnel endpoint directly.

| Argument | Default | Description |
| --- | --- | --- |
| `tunnel_endpoint` | Required | HTTP or HTTPS Tunnel endpoint for `MaxCompute`; ODPS service endpoint for `MaxComputeRaw`. Use the endpoint for the project's region and network. |
| `project` | Required | Remote MaxCompute project. |
| `table` | Required | Existing table in that project. |
| `partition` | Required | Tunnel partition specification, for example `ds=2026-09-18`. Use an empty string for a non-partitioned table. This is not a ClickHouse filter expression. |
| `access_key_id` | Required | Alibaba Cloud AccessKey ID. |
| `access_key_secret` | Required | Matching AccessKey secret. |
| `thread_num` | `1` | If local parallel reads are enabled, values greater than one request this number of streams; `0` or `1` uses the stream count requested by ClickHouse. It is not a guarantee of one reader. |
| `quota_name` | `'default'` | Tunnel quota. An empty string omits the SDK's quota request parameter. |
| `start` | `0` | Zero-based starting row in the download session. |
| `count` | `0` | Rows to read. Zero reads the remaining rows; a positive value is capped at the remaining row count. |

The legacy positional form with a numeric eighth argument is also accepted:

```sql
ENGINE = MaxCompute(
    'tunnel_endpoint', 'project', 'table', 'partition',
    'access_key_id', 'access_key_secret', thread_num, start [, count]
)
```

In this form the quota is `'default'`. A quoted eighth argument is always a quota name, even if it contains digits. Both forms are also supported by `MaxComputeRaw`.

The range applies to the remote download session before ClickHouse evaluates query filters. A start beyond the session's row count raises an exception; a start equal to the row count yields no rows. The range is not an `ORDER BY` or a stable pagination contract across separate queries.

### Using the ODPS router {#using-the-odps-router}

Use `MaxComputeRaw` when you have an ODPS service endpoint instead of a direct Tunnel endpoint:

```sql
CREATE TABLE remote_events_via_router
(
    id Int64,
    name Nullable(String)
)
ENGINE = MaxComputeRaw(
    'https://odps.example.com/api', 'example_project', 'events', '',
    'YOUR_ACCESS_KEY_ID', 'YOUR_ACCESS_KEY_SECRET'
);
```

Replace the endpoint and credential placeholders with your connection details. This engine has the same column mappings, read formats and settings as `MaxCompute`.

## Named collections {#named-collections}

Connection parameters can be stored in a [named collection](/concepts/features/configuration/server-config/named-collections):

```sql
CREATE NAMED COLLECTION maxcompute_connection AS
    endpoint = 'https://tunnel.example.com',
    project = 'example_project',
    `table` = 'events',
    access_key_id = 'YOUR_ACCESS_KEY_ID',
    access_key_secret = 'YOUR_ACCESS_KEY_SECRET';

CREATE TABLE remote_events
(
    id Int64,
    name Nullable(String)
)
ENGINE = MaxCompute(maxcompute_connection);
```

Replace the example endpoint and credential placeholders with your connection details. Named collection overrides use `key = value` and are subject to the collection's override policy.

| Key | Required / default | Description |
| --- | --- | --- |
| `endpoint` | Required | Endpoint selected according to `endpoint_mode`. |
| `project`, `table` | Required | Remote project and table. |
| `access_key_id`, `username`, or `user` | One required | AccessKey ID. |
| `access_key_secret`, `secret_access_key`, or `password` | One required | AccessKey secret. |
| `partition` or `partition_spec` | `''` | Tunnel partition specification. |
| `sts_token`, `session_token`, or `token` | `''` | Optional STS token. STS credentials must be supplied through a named collection. |
| `endpoint_mode` | `tunnel` for `MaxCompute`; `odps_router` for `MaxComputeRaw` | `tunnel` uses the endpoint directly; `odps_router` resolves a Tunnel endpoint first. |
| `thread_num`, `quota_name`, `start`, `count` | Same as positional arguments | Parallelism, quota and download range. |

Specify only one key from each alias group. Unknown keys and multiple aliases for the same parameter raise an exception.

## Reading and importing data {#reading-and-importing-data}

Assuming the remote `events` table has `id BIGINT` and `name STRING` columns:

```sql
SELECT id, name
FROM remote_events
ORDER BY id
LIMIT 10;

CREATE TABLE local_events
(
    id Int64,
    name Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO local_events
SELECT id, name FROM remote_events;
```

The engine reads the requested columns. It does not translate ClickHouse `WHERE`, `ORDER BY`, or aggregation expressions into remote SQL. Filtering and sorting happen in ClickHouse. Configure the remote partition in the engine arguments or named collection.

`INSERT SELECT` uses standard ClickHouse planning. The engine does not coordinate disjoint remote ranges across independent distributed queries. Do not assume that putting the same source on multiple shards automatically partitions the remote data.

## Read formats {#read-formats}

Use `maxcompute_read_format` to configure the read strategy, either as a table setting or as a query/session override. It does not change the remote table's storage format or select a different backend.

| Value | Scope | Behavior |
| --- | --- | --- |
| `inherit` | Query/session only (default) | Use each table's configured read strategy. A table without this setting uses `column`. |
| `row` | Table, query/session | Explicitly use the row reader. |
| `column` | Table, query/session | Use columnar reading through Arrow IPC. This is the table-level default. Require Arrow support and a supported mapping for the requested columns; otherwise raise an exception. |

### Table-level configuration {#table-level-configuration}

Both engines accept the table setting with either positional arguments or a named collection:

```sql
CREATE TABLE remote_events_column
(
    id Int64,
    name Nullable(String)
)
ENGINE = MaxCompute(maxcompute_connection)
SETTINGS maxcompute_read_format = 'column';

ALTER TABLE remote_events_column
MODIFY SETTING maxcompute_read_format = 'row';

ALTER TABLE remote_events_column
RESET SETTING maxcompute_read_format;
```

The setting is stored in the table metadata, appears in `SHOW CREATE TABLE`, and survives server restarts and table reattachment. `RESET SETTING` restores the table default, `column`. The value `inherit` is not accepted at table level. No additional positional engine argument or named collection key is required.

### Query and session overrides {#query-and-session-overrides}

An effective query/session value of `row` or `column` overrides the table setting. An effective value of `inherit` uses the table setting independently for each table. This allows one query to read different tables with different strategies.

```sql
SELECT id, name
FROM remote_events
SETTINGS maxcompute_read_format = 'column', odps_read_compress = 1;

SET maxcompute_read_format = 'row';

SELECT id, name
FROM remote_events_column
SETTINGS maxcompute_read_format = 'inherit';

SET maxcompute_read_format = 'inherit';
```

A query-level override takes precedence over the session/profile value for that query without changing it. In particular, query-level `inherit` bypasses a session-level override and uses the table setting. Session/profile settings used while creating a table are not captured as that table's default: use `CREATE TABLE ... SETTINGS` explicitly to store a default.

Format validation happens before reading and considers only the requested columns. Unsupported mappings, missing Arrow build support, and transport, checksum, authorization or conversion failures raise exceptions. The engine never switches to the other reader. For row-only mappings, explicitly configure `row`.

## Data types {#data-types}

The following are recommended declarations for the supported scalar mappings. Wrap scalar types in `Nullable` when remote values may be null.

| MaxCompute type | ClickHouse declaration |
| --- | --- |
| `BOOLEAN` | `UInt8` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `Int8`, `Int16`, `Int32`, `Int64`, respectively |
| `FLOAT`, `DOUBLE` | `Float32`, `Float64`, respectively |
| `STRING`, `VARCHAR`, `CHAR`, `BINARY` | `String` |
| `DECIMAL(p, s)` | `Decimal(p, s)` with a suitable precision and scale; `String` is also supported by the row reader |
| `DATE` | `Date` within its representable range |
| `DATETIME` | `DateTime`; subsecond precision is discarded |

Unsigned integer declarations require non-negative values within range. A null scalar cannot be read into a non-nullable column, even when `input_format_null_as_default = 1`. Nullable columns preserve NULL values.

For the supported `Date` and `DateTime` mappings, both readers reject values outside the target range, including temporal elements in arrays, maps and tuples. `Date` accepts day numbers from 0 to 65535 since the Unix epoch; `DateTime` accepts non-negative timestamps whose whole seconds fit in `UInt32`. Negative subsecond timestamps are rejected before truncation; valid subsecond precision is discarded. These checks cannot be disabled with `date_time_overflow_behavior = 'ignore'` or `'saturate'`.

Do not assume arbitrary casts between remote types and ClickHouse declarations are supported.

Complex columns support the following shapes for supported scalar elements:

| MaxCompute type | Example ClickHouse declaration |
| --- | --- |
| `ARRAY<BIGINT>` | `Array(Nullable(Int64))` |
| `MAP<STRING, BIGINT>` | `Nested(key Nullable(String), value Nullable(Int64))` |
| `STRUCT` with `BIGINT` and `STRING` fields | `Tuple(Nullable(Int64), Nullable(String))` in remote field order |

For a map column named `attrs`, use the flattened columns `attrs.key` and `attrs.value` produced by `Nested`. The engine does not treat a native ClickHouse `Map` declaration as this mapping. Array elements and tuple fields must be nullable.

Complex columns can also be declared as `String` for legacy row-reader rendering. This representation is not a JSON serialization contract: strings are not escaped, and empty containers render as an empty string. Such declarations require the row reader and are rejected by forced `column` mode (Arrow IPC).

Native mappings for `TIMESTAMP`, `TIMESTAMP_NTZ`, `INTERVAL_YEAR_MONTH`, and `INTERVAL_DAY_TIME` are not supported. Do not infer support for other types or arbitrary recursive nesting from the examples above.

## Query settings {#query-settings}

Set these options with `SET`, a settings profile, or a query-level `SETTINGS` clause. Only `maxcompute_read_format` also supports a table-level `CREATE TABLE ... SETTINGS` clause, with the separate default described above. The other options below are query/session settings only.

| Setting | Default | Effect |
| --- | --- | --- |
| `allow_experimental_maxcompute_storage_engine` | `0` | Allow creation of experimental `MaxCompute` and `MaxComputeRaw` tables, including user-supplied full `ATTACH TABLE` definitions. Existing metadata loading and reads are not gated. |
| `maxcompute_read_format` | `'inherit'` | Use the table strategy, or override it with `row` or `column` (Arrow IPC). The table-level default is `column`. |
| `odps_parallel_local_insert_select` | `1` | Enable local parallel reads for both `SELECT` and `INSERT SELECT`. Set to `0` to use a single reader. |
| `odps_read_compress` | `1` | Use ZLIB for row reads and ZSTD for Arrow reads. Set to `0` for uncompressed transfer. |
| `maxcompute_columnar_max_batch_bytes` | `0` | Arrow batch byte limit requested from the server; zero requests no limit. This is not a hard ClickHouse memory limit and does not apply to row reads. |
| `maxcompute_max_retries` | `3` | Maximum retries for retryable Tunnel reader failures; zero disables retries. |
| `maxcompute_retry_initial_backoff_ms` | `100` | Initial retry backoff in milliseconds. Must not exceed `maxcompute_retry_max_backoff_ms`. |
| `maxcompute_retry_max_backoff_ms` | `5000` | Maximum exponential retry backoff in milliseconds. |
| `maxcompute_retry_max_elapsed_ms` | `30000` | Cumulative budget for failed reader attempts and retry backoffs; zero disables retries. Not a total query timeout. |
| `maxcompute_connect_timeout_ms` | `10000` | SDK connection timeout in milliseconds. |
| `maxcompute_request_timeout_ms` | `300000` | SDK socket timeout in milliseconds. Not a total query timeout. |

SDK timeouts are rounded up to whole seconds with a minimum of one second, including when the configured value is zero. An in-flight SDK call is not interrupted by the retry budget; cancellation can wait for that call to return or time out.

The reader retries classified transport, timeout and throttling errors from the last consumed row or batch in the same download session. Checksum, schema and authorization failures are not retried by this policy. The policy covers reader operations, not the initial router request or download session creation.

### Internal and compatibility settings {#internal-and-compatibility-settings}

These settings are retained for compatibility with existing ODPS configurations. They do not enable the old ODPS-specific distributed execution path.

| Setting | Default | Status |
| --- | --- | --- |
| `enable_insert_from_odps_exteranl_table` | `1` | Historical misspelling, accepted with no planning effect. |
| `enable_insert_from_odps_external_table` | `1` | Correct spelling, also accepted with no planning effect; it is a separate setting. |
| `odps_parallel_distributed_insert_select` | `1` | Accepted with no planning effect. |
| `odps_distributed_insert_select_convert_to_local` | `1` | Accepted with no planning effect. |
| `odps_parallel_distributed_insert_select_start` | `0` | Internal range override. Zero uses the engine's `start`; configure ranges with engine arguments in user queries. |
| `odps_parallel_distributed_insert_select_count` | `0` | Internal range override. Zero uses the engine's `count`; configure ranges with engine arguments in user queries. |
| `odps_download_id` | `''` | Internal download-session identifier. An initial query with a non-empty value is rejected; leave empty in user profiles. |

## Access control and endpoints {#access-control-and-endpoints}

In addition to normal table privileges, creating these external tables is subject to ClickHouse's table-engine/source privilege checks. Both engines map to the `MAXCOMPUTE` source. An administrator can grant source privileges as follows:

```sql
GRANT READ, WRITE ON MAXCOMPUTE TO maxcompute_reader;
```

This grant does not implement writes to MaxCompute. Users still need the applicable `CREATE TABLE`, named collection and `SELECT` privileges for their operations. The remote AccessKey must independently have permission to read the MaxCompute data.

Configured endpoints and router-resolved Tunnel endpoints are checked against the server's [remote host allowlist](/reference/settings/server-settings/settings/remote#remote_url_allow_hosts). Allow the actual Tunnel host as well as the ODPS router host when using `MaxComputeRaw`. Configured endpoints must have an HTTP or HTTPS scheme and a host, and must not contain user information, a query string or a fragment. Prefer HTTPS; HTTP is accepted for existing configurations.
)DOCS_MD";

    factory.registerStorage("MaxCompute", [createStorageMaxCompute](const StorageFactory::Arguments & args)
    {
        return createStorageMaxCompute(args, "MaxCompute");
    }, {.supports_settings = true,
        .source_access_type = AccessTypeObjects::Source::MAXCOMPUTE,
        .has_builtin_setting_fn = hasMaxComputeSetting},
        Documentation{
            .description = description,
            .syntax = "MaxCompute(tunnel_endpoint, project, table, partition, access_key_id, access_key_secret [, thread_num [, quota_name [, start [, count]]]])",
            .examples = {{
                .name = "Read through a Tunnel endpoint",
                .query = R"(
SET allow_experimental_maxcompute_storage_engine = 1;
CREATE TABLE remote_events (id Int64, name Nullable(String))
ENGINE = MaxCompute('https://tunnel.example.com', 'example_project', 'events', '', 'YOUR_ACCESS_KEY_ID', 'YOUR_ACCESS_KEY_SECRET');
SELECT id, name FROM remote_events LIMIT 10;
)",
                .result = ""}},
            .introduced_in = {26, 9},
            .related = {"MaxComputeRaw"}});

    factory.registerStorage("MaxComputeRaw", [createStorageMaxCompute](const StorageFactory::Arguments & args)
    {
        return createStorageMaxCompute(args, "MaxComputeRaw");
    }, {.supports_settings = true,
        .source_access_type = AccessTypeObjects::Source::MAXCOMPUTE,
        .has_builtin_setting_fn = hasMaxComputeSetting},
        Documentation{
            .description = description,
            .syntax = "MaxComputeRaw(odps_endpoint, project, table, partition, access_key_id, access_key_secret [, thread_num [, quota_name [, start [, count]]]])",
            .examples = {{
                .name = "Read through the ODPS router",
                .query = R"(
SET allow_experimental_maxcompute_storage_engine = 1;
CREATE TABLE remote_events (id Int64, name Nullable(String))
ENGINE = MaxComputeRaw('https://odps.example.com/api', 'example_project', 'events', '', 'YOUR_ACCESS_KEY_ID', 'YOUR_ACCESS_KEY_SECRET');
SELECT id, name FROM remote_events LIMIT 10;
)",
                .result = ""}},
            .introduced_in = {26, 9},
            .related = {"MaxCompute"}});
}

}
#endif
