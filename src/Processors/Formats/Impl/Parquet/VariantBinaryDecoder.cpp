#include <Processors/Formats/Impl/Parquet/VariantBinaryDecoder.h>
#include <Processors/Formats/Impl/Parquet/UUIDUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>

#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>
#include <base/arithmeticOverflow.h>

#include <functional>
#include <unordered_set>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace DB::Parquet::VariantReader
{

namespace
{

void ensureVariantRemaining(const UInt8 * ptr, const UInt8 * end, size_t bytes, std::string_view what)
{
    if (static_cast<size_t>(end - ptr) < bytes)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: unexpected end of {}", what);
}

VariantBasicType decodeVariantBasicType(UInt8 header)
{
    switch (header & VARIANT_BASIC_TYPE_MASK)
    {
        case static_cast<UInt8>(VariantBasicType::Primitive):
            return VariantBasicType::Primitive;
        case static_cast<UInt8>(VariantBasicType::ShortString):
            return VariantBasicType::ShortString;
        case static_cast<UInt8>(VariantBasicType::Object):
            return VariantBasicType::Object;
        case static_cast<UInt8>(VariantBasicType::Array):
            return VariantBasicType::Array;
        default:
            UNREACHABLE();
    }
}

std::optional<VariantPrimitiveType> tryDecodeVariantPrimitiveType(UInt8 value_header)
{
    switch (value_header)
    {
        case static_cast<UInt8>(VariantPrimitiveType::Null):
            return VariantPrimitiveType::Null;
        case static_cast<UInt8>(VariantPrimitiveType::BooleanTrue):
            return VariantPrimitiveType::BooleanTrue;
        case static_cast<UInt8>(VariantPrimitiveType::BooleanFalse):
            return VariantPrimitiveType::BooleanFalse;
        case static_cast<UInt8>(VariantPrimitiveType::Int8):
            return VariantPrimitiveType::Int8;
        case static_cast<UInt8>(VariantPrimitiveType::Int16):
            return VariantPrimitiveType::Int16;
        case static_cast<UInt8>(VariantPrimitiveType::Int32):
            return VariantPrimitiveType::Int32;
        case static_cast<UInt8>(VariantPrimitiveType::Int64):
            return VariantPrimitiveType::Int64;
        case static_cast<UInt8>(VariantPrimitiveType::Double):
            return VariantPrimitiveType::Double;
        case static_cast<UInt8>(VariantPrimitiveType::Decimal4):
            return VariantPrimitiveType::Decimal4;
        case static_cast<UInt8>(VariantPrimitiveType::Decimal8):
            return VariantPrimitiveType::Decimal8;
        case static_cast<UInt8>(VariantPrimitiveType::Decimal16):
            return VariantPrimitiveType::Decimal16;
        case static_cast<UInt8>(VariantPrimitiveType::Date):
            return VariantPrimitiveType::Date;
        case static_cast<UInt8>(VariantPrimitiveType::TimestampMicros):
            return VariantPrimitiveType::TimestampMicros;
        case static_cast<UInt8>(VariantPrimitiveType::TimestampNtzMicros):
            return VariantPrimitiveType::TimestampNtzMicros;
        case static_cast<UInt8>(VariantPrimitiveType::Float):
            return VariantPrimitiveType::Float;
        case static_cast<UInt8>(VariantPrimitiveType::Binary):
            return VariantPrimitiveType::Binary;
        case static_cast<UInt8>(VariantPrimitiveType::String):
            return VariantPrimitiveType::String;
        case static_cast<UInt8>(VariantPrimitiveType::TimeNtzMicros):
            return VariantPrimitiveType::TimeNtzMicros;
        case static_cast<UInt8>(VariantPrimitiveType::TimestampNanos):
            return VariantPrimitiveType::TimestampNanos;
        case static_cast<UInt8>(VariantPrimitiveType::TimestampNtzNanos):
            return VariantPrimitiveType::TimestampNtzNanos;
        case static_cast<UInt8>(VariantPrimitiveType::UUID):
            return VariantPrimitiveType::UUID;
        default:
            return std::nullopt;
    }
}

UInt64 readVariantLittleEndianVariable(const UInt8 *& ptr, const UInt8 * end, size_t size, std::string_view what)
{
    ensureVariantRemaining(ptr, end, size, what);

    switch (size)
    {
        case 1:
            return *ptr++;
        case 2:
        {
            UInt16 value {};
            memcpy(&value, ptr, sizeof(value));
            transformEndianness<std::endian::native, std::endian::little>(value);
            ptr += sizeof(value);
            return value;
        }
        case 4:
        {
            UInt32 value {};
            memcpy(&value, ptr, sizeof(value));
            transformEndianness<std::endian::native, std::endian::little>(value);
            ptr += sizeof(value);
            return value;
        }
        case 8:
        {
            UInt64 value {};
            memcpy(&value, ptr, sizeof(value));
            transformEndianness<std::endian::native, std::endian::little>(value);
            ptr += sizeof(value);
            return value;
        }
        default:
        {
            UInt64 value = 0;
            for (size_t i = 0; i < size; ++i)
                value |= static_cast<UInt64>(ptr[i]) << (8 * i);
            ptr += size;
            return value;
        }
    }
}

void checkVariantObjectFieldCount(const FormatSettings & format_settings, UInt64 num_fields)
{
    if (format_settings.binary.max_object_size && num_fields > format_settings.binary.max_object_size)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too many fields in `Parquet` `VARIANT` object: {}. The maximum is: {}. To increase the maximum, use setting `format_binary_max_object_size`",
            num_fields,
            format_settings.binary.max_object_size);
    }
}

void checkVariantArraySize(const FormatSettings & format_settings, UInt64 num_elements)
{
    if (format_settings.binary.max_binary_array_size && num_elements > format_settings.binary.max_binary_array_size)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too large `Parquet` `VARIANT` array size: {}. The maximum is: {}. To increase the maximum, use setting `format_binary_max_array_size`",
            num_elements,
            format_settings.binary.max_binary_array_size);
    }
}

void checkVariantMetadataDictionarySize(const FormatSettings & format_settings, UInt64 dictionary_size)
{
    if (format_settings.binary.max_object_size && dictionary_size > format_settings.binary.max_object_size)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too large `Parquet` `VARIANT` metadata dictionary size: {}. The maximum is: {}. To increase the maximum, use setting `format_binary_max_object_size`",
            dictionary_size,
            format_settings.binary.max_object_size);
    }
}

void checkVariantStringSize(const FormatSettings & format_settings, UInt64 size)
{
    if (format_settings.binary.max_binary_string_size && size > format_settings.binary.max_binary_string_size)
    {
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Too large string size: {}. The maximum is: {}. To increase the maximum, use setting `format_binary_max_string_size`",
            size,
            format_settings.binary.max_binary_string_size);
    }
}

void checkVariantPayloadSize(const FormatSettings & format_settings, UInt64 size, std::string_view what)
{
    if (format_settings.binary.max_binary_string_size && size > format_settings.binary.max_binary_string_size)
    {
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Too large `Parquet` `VARIANT` {} size: {}. The maximum is: {}. To increase the maximum, use setting `format_binary_max_string_size`",
            what,
            size,
            format_settings.binary.max_binary_string_size);
    }
}

UInt64 checkedVariantBase64EncodedSize(UInt64 decoded_size)
{
    UInt64 adjusted_size = 0;
    if (common::addOverflow(decoded_size, UInt64{2}, adjusted_size))
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too large `Parquet` `VARIANT` binary size: {}", decoded_size);

    UInt64 encoded_size = 0;
    if (common::mulOverflow(adjusted_size / 3, UInt64{4}, encoded_size))
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too large `Parquet` `VARIANT` binary size: {}", decoded_size);

    return encoded_size;
}

size_t checkedVariantByteSize(UInt64 count, size_t element_size, std::string_view what)
{
    UInt64 total = 0;
    bool overflow = common::mulOverflow(count, static_cast<UInt64>(element_size), total);
    if constexpr (sizeof(size_t) < sizeof(UInt64))
        overflow = overflow || static_cast<UInt64>(static_cast<size_t>(total)) != total;

    if (overflow)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too large `Parquet` `VARIANT` {} size: {} items with element size {}",
            what,
            count,
            static_cast<UInt64>(element_size));
    }

    return static_cast<size_t>(total);
}

size_t checkedVariantByteSizeWithSentinel(UInt64 count, size_t element_size, std::string_view what)
{
    UInt64 count_with_sentinel = 0;
    if (common::addOverflow(count, UInt64{1}, count_with_sentinel))
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too large `Parquet` `VARIANT` {} size: {} items with element size {}",
            what,
            count,
            static_cast<UInt64>(element_size));
    }

    return checkedVariantByteSize(count_with_sentinel, element_size, what);
}

void checkVariantCollectionPayloadSize(UInt64 previous_offset, const UInt8 * values, const UInt8 * end, std::string_view what)
{
    if (previous_offset != static_cast<UInt64>(end - values))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid {} payload size", what);
}

template <typename T>
T readVariantPOD(const UInt8 *& ptr, const UInt8 * end, std::string_view what)
{
    ensureVariantRemaining(ptr, end, sizeof(T), what);

    T value {};
    memcpy(&value, ptr, sizeof(value));
    transformEndianness<std::endian::native, std::endian::little>(value);
    ptr += sizeof(value);
    return value;
}

UUID readVariantUUID(const UInt8 *& ptr, const UInt8 * end, std::string_view what)
{
    ensureVariantRemaining(ptr, end, sizeof(UUID), what);

    UUID value = decodeParquetUUID(ptr);
    ptr += sizeof(UUID);
    return value;
}

std::optional<UInt64> tryFindMetadataFieldId(const VariantMetadata & metadata, std::string_view name)
{
    if (metadata.strings_sorted)
    {
        auto it = std::lower_bound(metadata.strings.begin(), metadata.strings.end(), name);
        if (it != metadata.strings.end() && *it == name)
            return static_cast<UInt64>(it - metadata.strings.begin());
        return std::nullopt;
    }

    auto it = std::find(metadata.strings.begin(), metadata.strings.end(), name);
    if (it == metadata.strings.end())
        return std::nullopt;

    return static_cast<UInt64>(it - metadata.strings.begin());
}

void checkUniqueObjectFieldId(std::unordered_set<UInt64> & seen_field_ids, UInt64 field_id)
{
    if (!seen_field_ids.insert(field_id).second)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: duplicate object field id {}", field_id);
}

bool hasNestedPathComponent(const ParsedVariantPath & path, size_t step_idx)
{
    return step_idx + 1 < path.steps.size();
}

template <typename T>
struct VariantDecimalFieldTraits;

template <>
struct VariantDecimalFieldTraits<Int32>
{
    using FieldType = Decimal32;
};

template <>
struct VariantDecimalFieldTraits<Int64>
{
    using FieldType = Decimal64;
};

template <>
struct VariantDecimalFieldTraits<Int128>
{
    using FieldType = Decimal128;
};

template <typename ColumnType, typename ValueType>
ColumnPtr makeSingleValueColumn(const DataTypePtr & type, ValueType value)
{
    auto column = type->createColumn();
    assert_cast<ColumnType &>(*column).insertValue(value);
    return column;
}

ColumnPtr makeSingleStringColumn(const DataTypePtr & type, std::string_view value)
{
    auto column = type->createColumn();
    assert_cast<ColumnString &>(*column).insertData(value.data(), value.size());
    return column;
}

template <typename T>
void storeScalarPrimitiveValue(ScalarExactValue & result, ScalarExactValue::PrimitiveKind primitive_kind, T value)
{
    result.primitive_kind = primitive_kind;
    result.exact_column.reset();
    result.exact_row_num = 0;
    result.exact_string_view.reset();

    static_assert(sizeof(T) <= sizeof(result.primitive_bits));
    memcpy(&result.primitive_bits, &value, sizeof(T));
}

template <typename T>
T loadScalarPrimitiveValue(const ScalarExactValue & value)
{
    T result {};
    static_assert(sizeof(T) <= sizeof(value.primitive_bits));
    memcpy(&result, &value.primitive_bits, sizeof(T));
    return result;
}

ColumnPtr materializeScalarExactValueColumn(const ScalarExactValue & value)
{
    if (value.exact_column)
        return value.exact_column;

    if (value.exact_string_view.has_value())
        return makeSingleStringColumn(value.exact_type, *value.exact_string_view);

    switch (value.primitive_kind)
    {
        case ScalarExactValue::PrimitiveKind::UInt8:
            return makeSingleValueColumn<ColumnUInt8>(value.exact_type, loadScalarPrimitiveValue<UInt8>(value));
        case ScalarExactValue::PrimitiveKind::Int8:
            return makeSingleValueColumn<ColumnInt8>(value.exact_type, loadScalarPrimitiveValue<Int8>(value));
        case ScalarExactValue::PrimitiveKind::Int16:
            return makeSingleValueColumn<ColumnInt16>(value.exact_type, loadScalarPrimitiveValue<Int16>(value));
        case ScalarExactValue::PrimitiveKind::Int32:
            return makeSingleValueColumn<ColumnInt32>(value.exact_type, loadScalarPrimitiveValue<Int32>(value));
        case ScalarExactValue::PrimitiveKind::Int64:
            return makeSingleValueColumn<ColumnInt64>(value.exact_type, loadScalarPrimitiveValue<Int64>(value));
        case ScalarExactValue::PrimitiveKind::Float32:
            return makeSingleValueColumn<ColumnFloat32>(value.exact_type, loadScalarPrimitiveValue<Float32>(value));
        case ScalarExactValue::PrimitiveKind::Float64:
            return makeSingleValueColumn<ColumnFloat64>(value.exact_type, loadScalarPrimitiveValue<Float64>(value));
        case ScalarExactValue::PrimitiveKind::Date32:
            return makeSingleValueColumn<ColumnDate32>(value.exact_type, loadScalarPrimitiveValue<Int32>(value));
        case ScalarExactValue::PrimitiveKind::DateTime64:
            return makeSingleValueColumn<ColumnDateTime64>(value.exact_type, static_cast<DateTime64>(loadScalarPrimitiveValue<Int64>(value)));
        case ScalarExactValue::PrimitiveKind::Time64:
            return makeSingleValueColumn<ColumnTime64>(value.exact_type, static_cast<Time64>(loadScalarPrimitiveValue<Int64>(value)));
        case ScalarExactValue::PrimitiveKind::Decimal32:
            return makeSingleValueColumn<ColumnDecimal<Decimal32>>(value.exact_type, Decimal32(loadScalarPrimitiveValue<Int32>(value)));
        case ScalarExactValue::PrimitiveKind::Decimal64:
            return makeSingleValueColumn<ColumnDecimal<Decimal64>>(value.exact_type, Decimal64(loadScalarPrimitiveValue<Int64>(value)));
        case ScalarExactValue::PrimitiveKind::None:
            break;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot materialize `Parquet` `VARIANT` scalar exact value without payload");
}

VariantValue makeExactValue(ColumnPtr exact_column, DataTypePtr exact_type, size_t exact_row_num = 0)
{
    VariantValue value;
    value.kind = VariantValue::Kind::ExactValue;
    value.exact_type = std::move(exact_type);
    value.exact_column = std::move(exact_column);
    value.exact_row_num = exact_row_num;
    return value;
}

VariantValue makeExactStringValue(std::string_view exact_string_view, DataTypePtr exact_type)
{
    VariantValue value;
    value.kind = VariantValue::Kind::ExactValue;
    value.exact_type = std::move(exact_type);
    value.exact_string_view = exact_string_view;
    return value;
}

VariantValue makeExactScalarValue(ScalarExactValue exact_value)
{
    VariantValue value;
    value.kind = VariantValue::Kind::ExactValue;
    const bool has_string_view = exact_value.exact_string_view.has_value();
    if (!has_string_view)
        value.exact_column = materializeScalarExactValueColumn(exact_value);
    value.exact_type = std::move(exact_value.exact_type);
    value.exact_row_num = 0;
    value.exact_string_view = exact_value.exact_string_view;
    return value;
}

VariantValue makeArrayValue(std::vector<VariantValue> elements, DataTypePtr exact_type = {})
{
    VariantValue value;
    value.kind = VariantValue::Kind::Array;
    value.exact_type = std::move(exact_type);
    value.array_elements = std::move(elements);
    return value;
}

VariantValue makeObjectValue(std::map<String, VariantValue> fields, DataTypePtr exact_type = {})
{
    VariantValue value;
    value.kind = VariantValue::Kind::Object;
    value.exact_type = std::move(exact_type);
    value.object_fields = std::move(fields);
    return value;
}

template <typename T>
DecodedVariantValue decodeVariantScaledIntegerValue(T value, UInt8 scale)
{
    using DecimalFieldType = typename VariantDecimalFieldTraits<T>::FieldType;
    using DataType = DataTypeDecimal<DecimalFieldType>;
    auto exact_type = std::make_shared<DataType>(DataType::maxPrecision(), scale);
    return {.value = makeExactValue(makeSingleValueColumn<ColumnDecimal<DecimalFieldType>>(exact_type, DecimalFieldType(value)), exact_type)};
}

enum class ScalarExactDecodeStatus
{
    Exact,
    Null,
    Unsupported,
};

ScalarExactDecodeStatus tryDecodeScalarExactValueImpl(
    const UInt8 *& ptr,
    const UInt8 * end,
    ScalarExactValue & result,
    const FormatSettings & format_settings)
{
    ensureVariantRemaining(ptr, end, 1, "`VARIANT` value header");
    UInt8 header = *ptr++;

    VariantBasicType basic_type = decodeVariantBasicType(header);
    UInt8 value_header = header >> VARIANT_VALUE_HEADER_SHIFT;

    if (basic_type == VariantBasicType::ShortString)
    {
        UInt8 size = value_header;
        checkVariantStringSize(format_settings, size);
        ensureVariantRemaining(ptr, end, size, "`VARIANT` short string payload");
        result.exact_type = getParquetVariantScalarType(VariantPrimitiveType::String);
        result.primitive_kind = ScalarExactValue::PrimitiveKind::None;
        result.exact_column.reset();
        result.exact_row_num = 0;
        result.exact_string_view = std::string_view(reinterpret_cast<const char *>(ptr), size);
        ptr += size;
        return ScalarExactDecodeStatus::Exact;
    }

    if (basic_type != VariantBasicType::Primitive)
        return ScalarExactDecodeStatus::Unsupported;

    auto primitive_type_opt = tryDecodeVariantPrimitiveType(value_header);
    if (!primitive_type_opt)
        return ScalarExactDecodeStatus::Unsupported;

    VariantPrimitiveType primitive_type = *primitive_type_opt;
    switch (primitive_type)
    {
        case VariantPrimitiveType::Null:
            return ScalarExactDecodeStatus::Null;
        case VariantPrimitiveType::BooleanTrue:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::UInt8, UInt8(1));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::BooleanFalse:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::UInt8, UInt8(0));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::Int8:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Int8, readVariantPOD<Int8>(ptr, end, "`VARIANT` `INT8`"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::Int16:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Int16, readVariantPOD<Int16>(ptr, end, "`VARIANT` `INT16`"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::Int32:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Int32, readVariantPOD<Int32>(ptr, end, "`VARIANT` `INT32`"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::Int64:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Int64, readVariantPOD<Int64>(ptr, end, "`VARIANT` `INT64`"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::Float:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Float32, readVariantPOD<Float32>(ptr, end, "`VARIANT` `FLOAT`"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::Double:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Float64, readVariantPOD<Float64>(ptr, end, "`VARIANT` `DOUBLE`"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::Decimal4:
        {
            UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL4` scale");
            result.exact_type = getParquetVariantScalarType(primitive_type, scale);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Decimal32, readVariantPOD<Int32>(ptr, end, "`VARIANT` `DECIMAL4` value"));
            return ScalarExactDecodeStatus::Exact;
        }
        case VariantPrimitiveType::Decimal8:
        {
            UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL8` scale");
            result.exact_type = getParquetVariantScalarType(primitive_type, scale);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Decimal64, readVariantPOD<Int64>(ptr, end, "`VARIANT` `DECIMAL8` value"));
            return ScalarExactDecodeStatus::Exact;
        }
        case VariantPrimitiveType::Decimal16:
        {
            UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL16` scale");
            auto exact_type = getParquetVariantScalarType(primitive_type, scale);
            result.exact_type = exact_type;
            result.primitive_kind = ScalarExactValue::PrimitiveKind::None;
            result.exact_column = makeSingleValueColumn<ColumnDecimal<Decimal128>>(
                exact_type,
                Decimal128(readVariantPOD<Int128>(ptr, end, "`VARIANT` `DECIMAL16` value")));
            result.exact_row_num = 0;
            result.exact_string_view.reset();
            return ScalarExactDecodeStatus::Exact;
        }
        case VariantPrimitiveType::Date:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Date32, readVariantPOD<Int32>(ptr, end, "`VARIANT` `DATE`"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::TimestampMicros:
        case VariantPrimitiveType::TimestampNtzMicros:
        case VariantPrimitiveType::TimestampNanos:
        case VariantPrimitiveType::TimestampNtzNanos:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::DateTime64, readVariantPOD<Int64>(ptr, end, "`VARIANT` temporal primitive"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::TimeNtzMicros:
            result.exact_type = getParquetVariantScalarType(primitive_type);
            storeScalarPrimitiveValue(result, ScalarExactValue::PrimitiveKind::Time64, readVariantPOD<Int64>(ptr, end, "`VARIANT` temporal primitive"));
            return ScalarExactDecodeStatus::Exact;
        case VariantPrimitiveType::Binary:
        {
            UInt32 size = readVariantPOD<UInt32>(ptr, end, "`VARIANT` binary size");
            checkVariantStringSize(format_settings, size);
            checkVariantStringSize(format_settings, checkedVariantBase64EncodedSize(size));
            ensureVariantRemaining(ptr, end, size, "`VARIANT` binary payload");
            String encoded = base64Encode(String(reinterpret_cast<const char *>(ptr), size));
            ptr += size;
            auto exact_type = getParquetVariantScalarType(primitive_type);
            result.exact_type = exact_type;
            result.primitive_kind = ScalarExactValue::PrimitiveKind::None;
            result.exact_column = makeSingleStringColumn(exact_type, encoded);
            result.exact_row_num = 0;
            result.exact_string_view.reset();
            return ScalarExactDecodeStatus::Exact;
        }
        case VariantPrimitiveType::String:
        {
            UInt32 size = readVariantPOD<UInt32>(ptr, end, "`VARIANT` string size");
            checkVariantStringSize(format_settings, size);
            ensureVariantRemaining(ptr, end, size, "`VARIANT` string payload");
            result.exact_type = getParquetVariantScalarType(primitive_type);
            result.primitive_kind = ScalarExactValue::PrimitiveKind::None;
            result.exact_column.reset();
            result.exact_row_num = 0;
            result.exact_string_view = std::string_view(reinterpret_cast<const char *>(ptr), size);
            ptr += size;
            return ScalarExactDecodeStatus::Exact;
        }
        case VariantPrimitiveType::UUID:
        {
            auto exact_type = getParquetVariantScalarType(primitive_type);
            result.exact_type = exact_type;
            result.primitive_kind = ScalarExactValue::PrimitiveKind::None;
            result.exact_column = makeSingleValueColumn<ColumnUUID>(exact_type, readVariantUUID(ptr, end, "`VARIANT` `UUID`"));
            result.exact_row_num = 0;
            result.exact_string_view.reset();
            return ScalarExactDecodeStatus::Exact;
        }
    }

    return ScalarExactDecodeStatus::Unsupported;
}

const DataTypePtr & getParquetVariantJSONObjectType()
{
    static thread_local DataTypePtr type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
    return type;
}

DataTypePtr tryBuildExactArrayType(const std::vector<DecodedVariantValue> & elements)
{
    DataTypePtr element_type;
    bool has_nulls = false;

    for (const auto & element : elements)
    {
        if (element.value.isNull())
        {
            has_nulls = true;
            continue;
        }

        if (!element.value.exact_type)
            return {};

        if (!element_type)
        {
            element_type = element.value.exact_type;
            continue;
        }

        if (!element_type->equals(*element.value.exact_type))
            return {};
    }

    if (!element_type)
        return {};

    if (has_nulls)
        element_type = makeVariantExactOutputTypeNullable(element_type);

    return std::make_shared<DataTypeArray>(element_type);
}

DataTypePtr tryBuildExactArrayType(const std::vector<VariantValue> & elements)
{
    DataTypePtr element_type;
    bool has_nulls = false;

    for (const auto & element : elements)
    {
        if (element.isNull())
        {
            has_nulls = true;
            continue;
        }

        if (!element.exact_type)
            return {};

        if (!element_type)
        {
            element_type = element.exact_type;
            continue;
        }

        if (!element_type->equals(*element.exact_type))
            return {};
    }

    if (!element_type)
        return {};

    if (has_nulls)
        element_type = makeVariantExactOutputTypeNullable(element_type);

    return std::make_shared<DataTypeArray>(element_type);
}

DecodedVariantValue decodeValueImpl(const VariantMetadata & metadata, const UInt8 *& ptr, const UInt8 * end, const FormatSettings & format_settings, size_t depth)
{
    checkVariantReadDepth(format_settings, depth);

    {
        const UInt8 * scalar_ptr = ptr;
        ScalarExactValue exact_value;
        if (tryDecodeScalarExactValueImpl(scalar_ptr, end, exact_value, format_settings) == ScalarExactDecodeStatus::Exact)
        {
            ptr = scalar_ptr;
            return {.value = makeExactScalarValue(std::move(exact_value))};
        }
    }

    ensureVariantRemaining(ptr, end, 1, "`VARIANT` value header");
    UInt8 header = *ptr++;

    VariantBasicType basic_type = decodeVariantBasicType(header);
    UInt8 value_header = header >> VARIANT_VALUE_HEADER_SHIFT;

    switch (basic_type)
    {
        case VariantBasicType::Primitive:
        {
            auto primitive_type_opt = tryDecodeVariantPrimitiveType(value_header);
            if (!primitive_type_opt)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported `Parquet` `VARIANT` primitive tag: {}", static_cast<UInt32>(value_header));

            VariantPrimitiveType primitive_type = *primitive_type_opt;
            switch (primitive_type)
            {
                case VariantPrimitiveType::Null:
                    return {};
                case VariantPrimitiveType::BooleanTrue:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnUInt8>(exact_type, UInt8(1)), exact_type)};
                }
                case VariantPrimitiveType::BooleanFalse:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnUInt8>(exact_type, UInt8(0)), exact_type)};
                }
                case VariantPrimitiveType::Int8:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnInt8>(exact_type, readVariantPOD<Int8>(ptr, end, "`VARIANT` `INT8`")), exact_type)};
                }
                case VariantPrimitiveType::Int16:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnInt16>(exact_type, readVariantPOD<Int16>(ptr, end, "`VARIANT` `INT16`")), exact_type)};
                }
                case VariantPrimitiveType::Int32:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnInt32>(exact_type, readVariantPOD<Int32>(ptr, end, "`VARIANT` `INT32`")), exact_type)};
                }
                case VariantPrimitiveType::Int64:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnInt64>(exact_type, readVariantPOD<Int64>(ptr, end, "`VARIANT` `INT64`")), exact_type)};
                }
                case VariantPrimitiveType::Float:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnFloat32>(exact_type, readVariantPOD<Float32>(ptr, end, "`VARIANT` `FLOAT`")), exact_type)};
                }
                case VariantPrimitiveType::Double:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnFloat64>(exact_type, readVariantPOD<Float64>(ptr, end, "`VARIANT` `DOUBLE`")), exact_type)};
                }
                case VariantPrimitiveType::Decimal4:
                {
                    UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL4` scale");
                    return decodeVariantScaledIntegerValue(readVariantPOD<Int32>(ptr, end, "`VARIANT` `DECIMAL4` value"), scale);
                }
                case VariantPrimitiveType::Decimal8:
                {
                    UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL8` scale");
                    return decodeVariantScaledIntegerValue(readVariantPOD<Int64>(ptr, end, "`VARIANT` `DECIMAL8` value"), scale);
                }
                case VariantPrimitiveType::Decimal16:
                {
                    UInt8 scale = readVariantPOD<UInt8>(ptr, end, "`VARIANT` `DECIMAL16` scale");
                    return decodeVariantScaledIntegerValue(readVariantPOD<Int128>(ptr, end, "`VARIANT` `DECIMAL16` value"), scale);
                }
                case VariantPrimitiveType::Date:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    Int32 value = readVariantPOD<Int32>(ptr, end, "`VARIANT` `DATE`");
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnDate32>(exact_type, value), exact_type)};
                }
                case VariantPrimitiveType::TimestampMicros:
                case VariantPrimitiveType::TimestampNtzMicros:
                case VariantPrimitiveType::TimeNtzMicros:
                case VariantPrimitiveType::TimestampNanos:
                case VariantPrimitiveType::TimestampNtzNanos:
                {
                    const Int64 value = readVariantPOD<Int64>(ptr, end, "`VARIANT` temporal primitive");
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    if (primitive_type == VariantPrimitiveType::TimeNtzMicros)
                        return {.value = makeExactValue(makeSingleValueColumn<ColumnTime64>(exact_type, static_cast<Time64>(value)), exact_type)};

                    return {.value = makeExactValue(makeSingleValueColumn<ColumnDateTime64>(exact_type, static_cast<DateTime64>(value)), exact_type)};
                }
                case VariantPrimitiveType::Binary:
                {
                    UInt32 size = readVariantPOD<UInt32>(ptr, end, "`VARIANT` binary size");
                    checkVariantStringSize(format_settings, size);
                    checkVariantStringSize(format_settings, checkedVariantBase64EncodedSize(size));
                    ensureVariantRemaining(ptr, end, size, "`VARIANT` binary payload");
                    String encoded = base64Encode(String(reinterpret_cast<const char *>(ptr), size));
                    ptr += size;
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleStringColumn(exact_type, encoded), exact_type)};
                }
                case VariantPrimitiveType::String:
                {
                    UInt32 size = readVariantPOD<UInt32>(ptr, end, "`VARIANT` string size");
                    checkVariantStringSize(format_settings, size);
                    ensureVariantRemaining(ptr, end, size, "`VARIANT` string payload");
                    std::string_view value(reinterpret_cast<const char *>(ptr), size);
                    ptr += size;
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactStringValue(value, exact_type)};
                }
                case VariantPrimitiveType::UUID:
                {
                    auto exact_type = getParquetVariantScalarType(primitive_type);
                    return {.value = makeExactValue(makeSingleValueColumn<ColumnUUID>(exact_type, readVariantUUID(ptr, end, "`VARIANT` `UUID`")), exact_type)};
                }
            }

            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported `Parquet` `VARIANT` primitive tag: {}", static_cast<UInt32>(primitive_type));
        }
        case VariantBasicType::ShortString:
        {
            UInt8 size = value_header;
            checkVariantStringSize(format_settings, size);
            ensureVariantRemaining(ptr, end, size, "`VARIANT` short string payload");
            std::string_view value(reinterpret_cast<const char *>(ptr), size);
            ptr += size;
            auto exact_type = getParquetVariantScalarType(VariantPrimitiveType::String);
            return {.value = makeExactStringValue(value, exact_type)};
        }
        case VariantBasicType::Object:
        {
            size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
            size_t field_id_size = ((value_header >> VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT) & VARIANT_FIELD_ID_SIZE_MINUS_ONE_MASK) + 1;
            bool is_large = ((value_header >> VARIANT_OBJECT_IS_LARGE_SHIFT) & 0x01) != 0;
            UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` object field count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` object field count");
            checkVariantObjectFieldCount(format_settings, num_elements);

            const UInt8 * field_ids = ptr;
            size_t field_ids_bytes = checkedVariantByteSize(num_elements, field_id_size, "`object field ids`");
            ensureVariantRemaining(field_ids, end, field_ids_bytes, "`VARIANT` object field ids");
            ptr += field_ids_bytes;

            const UInt8 * field_offsets = ptr;
            size_t field_offsets_bytes = checkedVariantByteSizeWithSentinel(num_elements, field_offset_size, "`object field offsets`");
            ensureVariantRemaining(field_offsets, end, field_offsets_bytes, "`VARIANT` object field offsets");
            ptr += field_offsets_bytes;

            const UInt8 * values = ptr;
            const UInt8 * field_ids_ptr = field_ids;
            const UInt8 * offsets_ptr = field_offsets;
            UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");

            if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - values))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child offset");

            std::map<String, VariantValue> object;
            std::unordered_set<UInt64> seen_field_ids;
            seen_field_ids.reserve(static_cast<size_t>(num_elements));
            for (UInt64 i = 0; i < num_elements; ++i)
            {
                UInt64 field_id = readVariantLittleEndianVariable(field_ids_ptr, field_ids + field_ids_bytes, field_id_size, "`VARIANT` object field id");
                UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");
                if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values) || field_id >= metadata.strings.size())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child metadata");
                checkUniqueObjectFieldId(seen_field_ids, field_id);

                const UInt8 * child_ptr = values + previous_offset;
                auto child = decodeValueImpl(metadata, child_ptr, values + next_offset, format_settings, depth + 1);
                if (child_ptr != values + next_offset)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child size");

                String field_name(metadata.strings[field_id]);
                object[std::move(field_name)] = std::move(child.value);
                previous_offset = next_offset;
            }

            ptr = values + previous_offset;
            return {.value = makeObjectValue(std::move(object), getParquetVariantJSONObjectType())};
        }
        case VariantBasicType::Array:
        {
            size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
            bool is_large = ((value_header >> VARIANT_ARRAY_IS_LARGE_SHIFT) & 0x01) != 0;
            UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` array element count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` array element count");
            checkVariantArraySize(format_settings, num_elements);

            const UInt8 * field_offsets = ptr;
            size_t field_offsets_bytes = checkedVariantByteSizeWithSentinel(num_elements, field_offset_size, "`array offsets`");
            ensureVariantRemaining(field_offsets, end, field_offsets_bytes, "`VARIANT` array offsets");
            ptr += field_offsets_bytes;

            const UInt8 * values = ptr;
            const UInt8 * offsets_ptr = field_offsets;
            UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` array offset");

            if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - values))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child offset");

            std::vector<VariantValue> array;
            array.reserve(static_cast<size_t>(num_elements));
            std::vector<DecodedVariantValue> elements;
            elements.reserve(static_cast<size_t>(num_elements));
            for (UInt64 i = 0; i < num_elements; ++i)
            {
                UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` array offset");
                if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child size");

                const UInt8 * child_ptr = values + previous_offset;
                auto child = decodeValueImpl(metadata, child_ptr, values + next_offset, format_settings, depth + 1);
                array.emplace_back(child.value);
                elements.push_back(std::move(child));
                if (child_ptr != values + next_offset)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child size");

                previous_offset = next_offset;
            }

            ptr = values + previous_offset;
            return {.value = makeArrayValue(std::move(array), tryBuildExactArrayType(elements))};
        }
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: unexpected basic type tag");
}

void writeVariantValueAsJSON(const VariantValue & value, WriteBuffer & out, const FormatSettings & format_settings, size_t depth);

const VariantValue * tryGetSubcolumnValueImpl(const VariantValue & value, const ParsedVariantPath & path, const FormatSettings & format_settings, size_t depth, size_t step_idx)
{
    checkVariantReadDepth(format_settings, depth);

    if (step_idx >= path.steps.size())
        return &value;

    const auto & step = path.steps[step_idx];
    const bool has_nested_path = hasNestedPathComponent(path, step_idx);

    if (value.isObject())
    {
        auto exact_it = value.object_fields.find(step.remaining_path);
        if (exact_it != value.object_fields.end())
            return &exact_it->second;

        auto it = value.object_fields.find(step.key);
        if (it == value.object_fields.end())
            return nullptr;

        if (!has_nested_path)
            return &it->second;

        return tryGetSubcolumnValueImpl(it->second, path, format_settings, depth + 1, step_idx + 1);
    }

    if (value.isTuple())
    {
        for (const auto & [field_name, child] : value.tuple_elements)
        {
            if (field_name == step.remaining_path)
                return &child;
        }

        for (const auto & [field_name, child] : value.tuple_elements)
        {
            if (field_name != step.key)
                continue;

            if (!has_nested_path)
                return &child;

            return tryGetSubcolumnValueImpl(child, path, format_settings, depth + 1, step_idx + 1);
        }
    }

    return nullptr;
}

std::optional<VariantValue> extractSubcolumnValueImpl(const VariantValue & value, const ParsedVariantPath & path, const FormatSettings & format_settings, size_t depth, size_t step_idx)
{
    checkVariantReadDepth(format_settings, depth);

    if (step_idx >= path.steps.size())
        return value;

    const auto & step = path.steps[step_idx];
    const bool has_nested_path = hasNestedPathComponent(path, step_idx);

    auto extract_from_object_like = [&](const auto & fields) -> std::optional<VariantValue>
    {
        for (const auto & [field_name, child] : fields)
        {
            if (field_name == step.remaining_path)
                return child;
        }

        for (const auto & [field_name, child] : fields)
        {
            if (field_name != step.key)
                continue;

            if (!has_nested_path)
                return child;

            return extractSubcolumnValueImpl(child, path, format_settings, depth + 1, step_idx + 1);
        }

        return std::nullopt;
    };

    if (value.isObject())
        return extract_from_object_like(value.object_fields);

    if (value.isTuple())
        return extract_from_object_like(value.tuple_elements);

    if (value.isArray())
    {
        std::vector<VariantValue> projected_elements;
        projected_elements.reserve(value.array_elements.size());
        for (const auto & element : value.array_elements)
        {
            auto projected_element = extractSubcolumnValueImpl(element, path, format_settings, depth + 1, step_idx);
            projected_elements.emplace_back(projected_element ? std::move(*projected_element) : VariantValue {});
        }

        DataTypePtr exact_array_type = tryBuildExactArrayType(projected_elements);
        return makeArrayValue(std::move(projected_elements), std::move(exact_array_type));
    }

    return std::nullopt;
}

bool tryInsertFlatJSONPathImpl(std::map<String, VariantValue> & object, std::string_view path, VariantValue value, const FormatSettings & format_settings, size_t depth)
{
    checkVariantReadDepth(format_settings, depth);

    auto [head, tail] = Nested::splitName(path);
    String key = unescapeDotInJSONKey(String(head));

    if (tail.empty())
    {
        auto [_, inserted] = object.try_emplace(std::move(key), std::move(value));
        return inserted;
    }

    auto [it, inserted] = object.try_emplace(key, makeObjectValue({}));
    if (!inserted && !it->second.isObject())
        return false;

    return tryInsertFlatJSONPathImpl(it->second.object_fields, tail, std::move(value), format_settings, depth + 1);
}

std::optional<VariantValue> tryNestVariantJSONPaths(VariantValue value, const FormatSettings & format_settings, size_t depth)
{
    checkVariantReadDepth(format_settings, depth);

    if (value.isObject())
    {
        std::map<String, VariantValue> nested_object;
        auto object = std::move(value.object_fields);
        for (auto & [path, child] : object)
        {
            auto nested_child = tryNestVariantJSONPaths(child, format_settings, depth + 1);
            VariantValue child_value = nested_child.has_value() ? std::move(*nested_child) : std::move(child);

            if (!tryInsertFlatJSONPathImpl(nested_object, path, std::move(child_value), format_settings, depth))
                return std::nullopt;
        }

        value.object_fields = std::move(nested_object);
        return value;
    }

    if (value.isArray())
    {
        for (auto & element : value.array_elements)
        {
            auto nested_element = tryNestVariantJSONPaths(element, format_settings, depth + 1);
            if (nested_element.has_value())
                element = std::move(*nested_element);
        }
        return value;
    }

    if (value.isTuple())
    {
        for (auto & [_, child] : value.tuple_elements)
        {
            auto nested_child = tryNestVariantJSONPaths(child, format_settings, depth + 1);
            if (nested_child.has_value())
                child = std::move(*nested_child);
        }
        return value;
    }

    return value;
}

void writeVariantValueAsJSON(const VariantValue & value, WriteBuffer & out, const FormatSettings & format_settings, size_t depth)
{
    checkVariantReadDepth(format_settings, depth);

    switch (value.kind)
    {
        case VariantValue::Kind::Null:
            writeCString("null", out);
            return;
        case VariantValue::Kind::ExactValue:
            if (!value.exact_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize `Parquet` `VARIANT` exact value without payload");

            if (value.exact_string_view.has_value())
            {
                writeJSONString(*value.exact_string_view, out, format_settings);
                return;
            }

            if (!value.exact_column)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize `Parquet` `VARIANT` exact value without payload");

            value.exact_type->getDefaultSerialization()->serializeTextJSON(*value.exact_column, value.exact_row_num, out, format_settings);
            return;
        case VariantValue::Kind::Array:
            writeChar('[', out);
            for (size_t i = 0; i < value.array_elements.size(); ++i)
            {
                if (i != 0)
                    writeChar(',', out);
                writeVariantValueAsJSON(value.array_elements[i], out, format_settings, depth + 1);
            }
            writeChar(']', out);
            return;
        case VariantValue::Kind::Object:
            writeChar('{', out);
            for (auto it = value.object_fields.begin(); it != value.object_fields.end(); ++it)
            {
                if (it != value.object_fields.begin())
                    writeChar(',', out);
                writeJSONString(it->first, out, format_settings);
                writeChar(':', out);
                writeVariantValueAsJSON(it->second, out, format_settings, depth + 1);
            }
            writeChar('}', out);
            return;
        case VariantValue::Kind::Tuple:
            writeChar('{', out);
            for (size_t i = 0; i < value.tuple_elements.size(); ++i)
            {
                if (i != 0)
                    writeChar(',', out);
                writeJSONString(value.tuple_elements[i].first, out, format_settings);
                writeChar(':', out);
                writeVariantValueAsJSON(value.tuple_elements[i].second, out, format_settings, depth + 1);
            }
            writeChar('}', out);
            return;
    }
}

MutableColumnPtr materializeVariantValueColumnImpl(const VariantValue & value, const DataTypePtr & type, const FormatSettings & format_settings, size_t depth);

MutableColumnPtr materializeSingleExactValueColumn(const VariantValue & value)
{
    if (!value.exact_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot materialize `Parquet` `VARIANT` exact value without payload");

    auto column = value.exact_type->createColumn();
    if (value.exact_column)
    {
        column->insertFrom(*value.exact_column, value.exact_row_num);
        return column;
    }

    if (value.exact_string_view.has_value())
    {
        assert_cast<ColumnString &>(*column).insertData(value.exact_string_view->data(), value.exact_string_view->size());
        return column;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot materialize `Parquet` `VARIANT` exact value without payload");
}

bool tryInsertObjectLikeValueIntoTypedColumn(
    IColumn & column,
    const DataTypeObject & object_type,
    const VariantValue & value,
    const FormatSettings & format_settings,
    size_t depth)
{
    if (!value.isObjectLike())
        return false;

    auto & object_column = assert_cast<ColumnObject &>(column);
    const size_t row_start_size = object_column.size();

    auto append_key = [](std::string_view current_path, std::string_view key, bool is_root)
    {
        String path(current_path);
        if (!is_root)
            path.push_back('.');
        path += escapeDotInJSONKey(String(key));
        return path;
    };

    std::function<bool(const VariantValue &, std::string_view, size_t, bool)> append_paths;
    append_paths = [&](const VariantValue & current_value, std::string_view current_path, size_t current_depth, bool is_root) -> bool
    {
        checkVariantReadDepth(format_settings, current_depth);

        if (current_value.isObject())
        {
            for (const auto & [key, child] : current_value.object_fields)
            {
                if (!append_paths(child, append_key(current_path, key, is_root), current_depth + 1, false))
                    return false;
            }

            return true;
        }

        if (current_value.isTuple())
        {
            for (const auto & [key, child] : current_value.tuple_elements)
            {
                if (!append_paths(child, append_key(current_path, key, is_root), current_depth + 1, false))
                    return false;
            }

            return true;
        }

        const String path(current_path);
        if (auto typed_it = object_type.getTypedPaths().find(path); typed_it != object_type.getTypedPaths().end())
        {
            auto value_column = materializeVariantValueColumnImpl(current_value, typed_it->second, format_settings, current_depth + 1);
            object_column.getTypedPaths().at(path)->insertFrom(*value_column, 0);
            return true;
        }

        auto dynamic_value_column = materializeVariantValueColumnImpl(current_value, object_type.getDynamicType(), format_settings, current_depth + 1);
        const auto & dynamic_value = assert_cast<const ColumnDynamic &>(*dynamic_value_column);
        if (auto dynamic_it = object_column.getDynamicPathsPtrs().find(path); dynamic_it != object_column.getDynamicPathsPtrs().end())
        {
            dynamic_it->second->insertFrom(dynamic_value, 0);
            return true;
        }

        if (auto * new_dynamic_path = object_column.tryToAddNewDynamicPath(path))
        {
            new_dynamic_path->insertFrom(dynamic_value, 0);
            return true;
        }

        auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
        ColumnObject::serializePathAndValueIntoSharedData(shared_data_paths, shared_data_values, path, dynamic_value, 0);
        return true;
    };

    if (!append_paths(value, std::string_view {}, depth, true))
        return false;

    auto [shared_data_paths, _] = object_column.getSharedDataPathsAndValues();
    object_column.getSharedDataOffsets().push_back(shared_data_paths->size());

    for (auto & [_, subcolumn] : object_column.getTypedPaths())
    {
        if (subcolumn->size() == row_start_size)
            subcolumn->insertDefault();
    }

    for (auto & [_, subcolumn] : object_column.getDynamicPathsPtrs())
    {
        if (subcolumn->size() == row_start_size)
            subcolumn->insertDefault();
    }

    return true;
}

void insertVariantValueIntoTypedColumn(
    IColumn & column,
    const DataTypePtr & type,
    const VariantValue & value,
    const FormatSettings & format_settings,
    size_t depth)
{
    checkVariantReadDepth(format_settings, depth);

    if (value.isNull())
    {
        column.insertDefault();
        return;
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto & nullable_column = assert_cast<ColumnNullable &>(column);
        insertVariantValueIntoTypedColumn(nullable_column.getNestedColumn(), nullable_type->getNestedType(), value, format_settings, depth + 1);
        nullable_column.getNullMapData().push_back(UInt8(0));
        return;
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        auto materialized = materializeVariantValueColumnImpl(value, low_cardinality_type->getDictionaryType(), format_settings, depth + 1);
        auto casted = castColumn({std::move(materialized), low_cardinality_type->getDictionaryType(), "__parquet_variant_exact_value"}, type);
        column.insertFrom(*casted->convertToFullColumnIfConst(), 0);
        return;
    }

    if (typeid_cast<const DataTypeDynamic *>(type.get()))
    {
        auto & dynamic_column = assert_cast<ColumnDynamic &>(column);

        if (value.exact_type && !value.exact_string_view.has_value() && value.exact_column)
        {
            dynamic_column.insertTypedValueFrom(*value.exact_column, value.exact_type, value.exact_row_num);
            return;
        }

        auto source_type = value.exact_type ? value.exact_type : inferVariantMaterializationType(value, format_settings, depth + 1);
        auto source_column = materializeVariantValueColumnImpl(value, source_type, format_settings, depth + 1);
        dynamic_column.insertTypedValueFrom(*source_column, source_type, 0);
        return;
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        auto source_type = value.exact_type ? value.exact_type : inferVariantMaterializationType(value, format_settings, depth + 1);
        if (auto discr = variant_type->tryGetVariantDiscriminator(source_type->getName()))
        {
            auto & variant_column = assert_cast<ColumnVariant &>(column);
            if (value.exact_type && !value.exact_string_view.has_value() && value.exact_column && source_type->equals(*value.exact_type))
            {
                variant_column.insertIntoVariantFrom(*discr, *value.exact_column, value.exact_row_num);
                return;
            }

            auto source_column = materializeVariantValueColumnImpl(value, source_type, format_settings, depth + 1);
            variant_column.insertIntoVariantFrom(*discr, *source_column, 0);
            return;
        }

        auto source_column = materializeVariantValueColumnImpl(value, source_type, format_settings, depth + 1);
        auto casted = castColumn({std::move(source_column), source_type, "__parquet_variant_value"}, type);
        column.insertFrom(*casted->convertToFullColumnIfConst(), 0);
        return;
    }

    if (value.isExactValue())
    {
        if (!value.exact_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot materialize `Parquet` `VARIANT` exact value without payload");

        if (value.exact_string_view.has_value())
        {
            if (value.exact_type->equals(*type))
            {
                assert_cast<ColumnString &>(column).insertData(value.exact_string_view->data(), value.exact_string_view->size());
                return;
            }

            auto casted = castColumn({materializeSingleExactValueColumn(value), value.exact_type, "__parquet_variant_exact_value"}, type);
            column.insertFrom(*casted->convertToFullColumnIfConst(), 0);
            return;
        }

        if (value.exact_type->equals(*type))
        {
            column.insertFrom(*value.exact_column, value.exact_row_num);
            return;
        }

        auto casted = castColumn({materializeSingleExactValueColumn(value), value.exact_type, "__parquet_variant_exact_value"}, type);
        column.insertFrom(*casted->convertToFullColumnIfConst(), 0);
        return;
    }

    if (typeid_cast<const DataTypeString *>(type.get()))
    {
        String json;
        WriteBufferFromString buffer(json);
        writeVariantValueAsJSON(value, buffer, format_settings, depth + 1);
        column.insertData(json.data(), json.size());
        return;
    }

    if (const auto * object_type = typeid_cast<const DataTypeObject *>(type.get()))
    {
        if (!tryInsertObjectLikeValueIntoTypedColumn(column, *object_type, value, format_settings, depth + 1))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected `Parquet` `VARIANT` object-like value while materializing exact type {}",
                type->getName());
        }
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        if (!value.isArray())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected `Parquet` `VARIANT` array while materializing exact type {}", type->getName());

        auto & array_column = assert_cast<ColumnArray &>(column);
        auto & offsets = array_column.getOffsets();
        size_t next_offset = offsets.empty() ? 0 : offsets.back();
        for (const auto & element : value.array_elements)
        {
            insertVariantValueIntoTypedColumn(array_column.getData(), array_type->getNestedType(), element, format_settings, depth + 1);
            ++next_offset;
        }
        offsets.push_back(next_offset);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        if (!value.isTuple())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected `Parquet` `VARIANT` tuple while materializing exact type {}", type->getName());

        auto & tuple_column = assert_cast<ColumnTuple &>(column);
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            const auto & field_name = tuple_type->getNameByPosition(i + 1);
            auto it = std::find_if(
                value.tuple_elements.begin(),
                value.tuple_elements.end(),
                [&](const auto & element) { return element.first == field_name; });

            if (it == value.tuple_elements.end())
                tuple_column.getColumn(i).insertDefault();
            else
                insertVariantValueIntoTypedColumn(tuple_column.getColumn(i), tuple_type->getElement(i), it->second, format_settings, depth + 1);
        }

        if (tuple_type->getElements().empty())
            tuple_column.addSize(1);
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported exact `Parquet` `VARIANT` materialization for type {}", type->getName());
}

MutableColumnPtr materializeVariantValueColumnImpl(const VariantValue & value, const DataTypePtr & type, const FormatSettings & format_settings, size_t depth)
{
    auto column = type->createColumn();
    insertVariantValueIntoTypedColumn(*column, type, value, format_settings, depth);
    return column;
}

}

const VariantMetadata & VariantMetadataCache::get(std::string_view metadata_blob, const FormatSettings & format_settings)
{
    checkVariantPayloadSize(format_settings, metadata_blob.size(), "`metadata` blob");

    auto [it, inserted] = cache.try_emplace(String(metadata_blob), VariantMetadata {});
    if (inserted)
        it->second = decodeMetadata(it->first, format_settings);

    return it->second;
}

ParsedVariantPath parseVariantPath(std::string_view path)
{
    ParsedVariantPath parsed_path;
    while (!path.empty())
    {
        auto [head, tail] = Nested::splitName(path);

        ParsedVariantPathStep step;
        step.remaining_path = path;
        step.key = unescapeDotInJSONKey(String(head));
        parsed_path.steps.push_back(std::move(step));

        path = tail;
    }

    return parsed_path;
}

ResolvedVariantPath resolveVariantPath(const VariantMetadata & metadata, const ParsedVariantPath & path)
{
    ResolvedVariantPath resolved_path;
    resolved_path.parsed_path = path;
    resolved_path.steps.reserve(path.steps.size());

    for (const auto & step : path.steps)
    {
        ResolvedVariantPathStep resolved_step;
        resolved_step.exact_match_field_id = tryFindMetadataFieldId(metadata, step.remaining_path);
        resolved_step.nested_key_field_id = tryFindMetadataFieldId(metadata, step.key);
        resolved_path.steps.push_back(std::move(resolved_step));
    }

    return resolved_path;
}

VariantMetadata decodeMetadata(std::string_view metadata_blob, const FormatSettings & format_settings)
{
    if (metadata_blob.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: empty `metadata` blob");

    checkVariantPayloadSize(format_settings, metadata_blob.size(), "`metadata` blob");

    const auto * ptr = reinterpret_cast<const UInt8 *>(metadata_blob.data());
    const auto * end = ptr + metadata_blob.size();

    UInt8 header = *ptr++;
    UInt8 version = header & VARIANT_METADATA_VERSION_MASK;
    if (version != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported `Parquet` `VARIANT` metadata version: {}", static_cast<UInt32>(version));

    size_t offset_size = ((header >> VARIANT_METADATA_OFFSET_SIZE_MINUS_ONE_SHIFT) & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
    UInt64 dictionary_size = readVariantLittleEndianVariable(ptr, end, offset_size, "`VARIANT` metadata dictionary size");
    checkVariantMetadataDictionarySize(format_settings, dictionary_size);

    const UInt8 * offsets = ptr;
    size_t offsets_bytes = checkedVariantByteSizeWithSentinel(dictionary_size, offset_size, "`metadata offsets`");
    ensureVariantRemaining(offsets, end, offsets_bytes, "`VARIANT` metadata offsets");
    ptr += offsets_bytes;

    const UInt8 * strings_data = ptr;
    const UInt8 * offsets_ptr = offsets;
    UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, offsets + offsets_bytes, offset_size, "`VARIANT` metadata offset");
    if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - strings_data))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid `metadata` first offset");

    VariantMetadata metadata;
    metadata.strings_sorted = ((header >> VARIANT_METADATA_SORTED_STRINGS_SHIFT) & 0x01) != 0;
    metadata.strings.reserve(dictionary_size);
    for (UInt64 i = 0; i < dictionary_size; ++i)
    {
        UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, offsets + offsets_bytes, offset_size, "`VARIANT` metadata offset");
        if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - strings_data))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid `metadata` string offsets");

        checkVariantPayloadSize(format_settings, next_offset - previous_offset, "`metadata` string");

        metadata.strings.emplace_back(reinterpret_cast<const char *>(strings_data + previous_offset), static_cast<size_t>(next_offset - previous_offset));
        previous_offset = next_offset;
    }

    if (previous_offset != static_cast<UInt64>(end - strings_data))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid `metadata` payload size");

    std::unordered_set<String> unique_strings;
    unique_strings.reserve(metadata.strings.size());
    for (const auto & string : metadata.strings)
    {
        if (!unique_strings.emplace(string).second)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: duplicate `metadata` dictionary string '{}'", string);
    }

    if (metadata.strings_sorted && !std::is_sorted(metadata.strings.begin(), metadata.strings.end()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: unsorted `metadata` dictionary with sorted flag set");

    return metadata;
}

DecodedVariantValue decodeValue(const VariantMetadata & metadata, std::string_view value_blob, const FormatSettings & format_settings)
{
    checkVariantPayloadSize(format_settings, value_blob.size(), "`value` blob");

    const auto * begin = reinterpret_cast<const UInt8 *>(value_blob.data());
    const auto * end = begin + value_blob.size();

    const UInt8 * consumed = begin;
    auto value = decodeValueImpl(metadata, consumed, end, format_settings, 1);
    if (consumed != end)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: trailing bytes in `value` blob");

    return value;
}

static bool tryDecodeValueByPathImpl(
    const VariantMetadata & metadata,
    const UInt8 * begin,
    const UInt8 * end,
    const ParsedVariantPath & path,
    VariantValue & result,
    const FormatSettings & format_settings,
    size_t depth,
    size_t step_idx)
{
    checkVariantReadDepth(format_settings, depth);

    if (step_idx >= path.steps.size())
    {
        const UInt8 * ptr = begin;
        auto value = decodeValueImpl(metadata, ptr, end, format_settings, depth);
        if (ptr != end)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid child size while decoding path");
        result = std::move(value.value);
        return true;
    }

    ensureVariantRemaining(begin, end, 1, "`VARIANT` value header");
    const UInt8 * ptr = begin;
    UInt8 header = *ptr++;

    VariantBasicType basic_type = decodeVariantBasicType(header);
    UInt8 value_header = header >> VARIANT_VALUE_HEADER_SHIFT;

    if (basic_type == VariantBasicType::Array)
    {
        size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
        bool is_large = ((value_header >> VARIANT_ARRAY_IS_LARGE_SHIFT) & 0x01) != 0;
        UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` array element count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` array element count");
        checkVariantArraySize(format_settings, num_elements);

        const UInt8 * field_offsets = ptr;
        size_t field_offsets_bytes = checkedVariantByteSizeWithSentinel(num_elements, field_offset_size, "`array offsets`");
        ensureVariantRemaining(field_offsets, end, field_offsets_bytes, "`VARIANT` array offsets");
        ptr += field_offsets_bytes;

        const UInt8 * values = ptr;
        const UInt8 * offsets_ptr = field_offsets;
        UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` array offset");

        if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - values))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child offset");

        std::vector<VariantValue> projected_elements;
        projected_elements.reserve(static_cast<size_t>(num_elements));
        for (UInt64 i = 0; i < num_elements; ++i)
        {
            UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` array offset");
            if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child size");

            const UInt8 * child_begin = values + previous_offset;
            const UInt8 * child_end = values + next_offset;

            VariantValue child_result;
            bool decoded = tryDecodeValueByPathImpl(metadata, child_begin, child_end, path, child_result, format_settings, depth + 1, step_idx);
            projected_elements.emplace_back(decoded ? std::move(child_result) : VariantValue {});
            previous_offset = next_offset;
        }

        checkVariantCollectionPayloadSize(previous_offset, values, end, "`array`");

        DataTypePtr exact_array_type = tryBuildExactArrayType(projected_elements);
        result = makeArrayValue(std::move(projected_elements), std::move(exact_array_type));
        return true;
    }

    if (basic_type != VariantBasicType::Object)
    {
        const UInt8 * value_ptr = begin;
        decodeValueImpl(metadata, value_ptr, end, format_settings, depth);
        if (value_ptr != end)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid value size while decoding path");
        return false;
    }

    size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
    size_t field_id_size = ((value_header >> VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT) & VARIANT_FIELD_ID_SIZE_MINUS_ONE_MASK) + 1;
    bool is_large = ((value_header >> VARIANT_OBJECT_IS_LARGE_SHIFT) & 0x01) != 0;
    UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` object field count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` object field count");
    checkVariantObjectFieldCount(format_settings, num_elements);

    const UInt8 * field_ids = ptr;
    size_t field_ids_bytes = checkedVariantByteSize(num_elements, field_id_size, "`object field ids`");
    ensureVariantRemaining(field_ids, end, field_ids_bytes, "`VARIANT` object field ids");
    ptr += field_ids_bytes;

    const UInt8 * field_offsets = ptr;
    size_t field_offsets_bytes = checkedVariantByteSizeWithSentinel(num_elements, field_offset_size, "`object field offsets`");
    ensureVariantRemaining(field_offsets, end, field_offsets_bytes, "`VARIANT` object field offsets");
    ptr += field_offsets_bytes;

    const UInt8 * values = ptr;
    const UInt8 * field_ids_ptr = field_ids;
    const UInt8 * offsets_ptr = field_offsets;
    UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");

    if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - values))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child offset");

    const auto & step = path.steps[step_idx];
    const bool has_nested_path = hasNestedPathComponent(path, step_idx);

    const UInt8 * nested_begin = nullptr;
    const UInt8 * nested_end = nullptr;
    const UInt8 * exact_begin = nullptr;
    const UInt8 * exact_end = nullptr;

    std::unordered_set<UInt64> seen_field_ids;
    seen_field_ids.reserve(static_cast<size_t>(num_elements));
    for (UInt64 i = 0; i < num_elements; ++i)
    {
        UInt64 field_id = readVariantLittleEndianVariable(field_ids_ptr, field_ids + field_ids_bytes, field_id_size, "`VARIANT` object field id");
        UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");
        if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values) || field_id >= metadata.strings.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child metadata");
        checkUniqueObjectFieldId(seen_field_ids, field_id);

        const UInt8 * child_begin = values + previous_offset;
        const UInt8 * child_end = values + next_offset;
        std::string_view field_name = metadata.strings[field_id];

        if (!exact_begin && field_name == step.remaining_path)
        {
            exact_begin = child_begin;
            exact_end = child_end;
        }

        if (!nested_begin && has_nested_path && field_name == step.key)
        {
            nested_begin = child_begin;
            nested_end = child_end;
        }

        previous_offset = next_offset;
    }

    checkVariantCollectionPayloadSize(previous_offset, values, end, "`object`");

    if (exact_begin)
        return tryDecodeValueByPathImpl(metadata, exact_begin, exact_end, path, result, format_settings, depth + 1, path.steps.size());

    if (nested_begin)
        return tryDecodeValueByPathImpl(metadata, nested_begin, nested_end, path, result, format_settings, depth + 1, step_idx + 1);

    return false;
}

static bool tryDecodeValueByPathImpl(
    const VariantMetadata & metadata,
    const UInt8 * begin,
    const UInt8 * end,
    const ResolvedVariantPath & path,
    VariantValue & result,
    const FormatSettings & format_settings,
    size_t depth,
    size_t step_idx)
{
    checkVariantReadDepth(format_settings, depth);

    if (step_idx >= path.steps.size())
    {
        const UInt8 * ptr = begin;
        auto value = decodeValueImpl(metadata, ptr, end, format_settings, depth);
        if (ptr != end)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid child size while decoding path");
        result = std::move(value.value);
        return true;
    }

    ensureVariantRemaining(begin, end, 1, "`VARIANT` value header");
    const UInt8 * ptr = begin;
    UInt8 header = *ptr++;

    VariantBasicType basic_type = decodeVariantBasicType(header);
    UInt8 value_header = header >> VARIANT_VALUE_HEADER_SHIFT;

    if (basic_type == VariantBasicType::Array)
    {
        size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
        bool is_large = ((value_header >> VARIANT_ARRAY_IS_LARGE_SHIFT) & 0x01) != 0;
        UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` array element count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` array element count");
        checkVariantArraySize(format_settings, num_elements);

        const UInt8 * field_offsets = ptr;
        size_t field_offsets_bytes = checkedVariantByteSizeWithSentinel(num_elements, field_offset_size, "`array offsets`");
        ensureVariantRemaining(field_offsets, end, field_offsets_bytes, "`VARIANT` array offsets");
        ptr += field_offsets_bytes;

        const UInt8 * values = ptr;
        const UInt8 * offsets_ptr = field_offsets;
        UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` array offset");

        if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - values))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child offset");

        std::vector<VariantValue> projected_elements;
        projected_elements.reserve(static_cast<size_t>(num_elements));
        for (UInt64 i = 0; i < num_elements; ++i)
        {
            UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` array offset");
            if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid array child size");

            const UInt8 * child_begin = values + previous_offset;
            const UInt8 * child_end = values + next_offset;

            VariantValue child_result;
            bool decoded = tryDecodeValueByPathImpl(metadata, child_begin, child_end, path, child_result, format_settings, depth + 1, step_idx);
            projected_elements.emplace_back(decoded ? std::move(child_result) : VariantValue {});
            previous_offset = next_offset;
        }

        checkVariantCollectionPayloadSize(previous_offset, values, end, "`array`");

        DataTypePtr exact_array_type = tryBuildExactArrayType(projected_elements);
        result = makeArrayValue(std::move(projected_elements), std::move(exact_array_type));
        return true;
    }

    if (basic_type != VariantBasicType::Object)
    {
        const UInt8 * value_ptr = begin;
        decodeValueImpl(metadata, value_ptr, end, format_settings, depth);
        if (value_ptr != end)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid value size while decoding path");
        return false;
    }

    size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
    size_t field_id_size = ((value_header >> VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT) & VARIANT_FIELD_ID_SIZE_MINUS_ONE_MASK) + 1;
    bool is_large = ((value_header >> VARIANT_OBJECT_IS_LARGE_SHIFT) & 0x01) != 0;
    UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` object field count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` object field count");
    checkVariantObjectFieldCount(format_settings, num_elements);

    const UInt8 * field_ids = ptr;
    size_t field_ids_bytes = checkedVariantByteSize(num_elements, field_id_size, "`object field ids`");
    ensureVariantRemaining(field_ids, end, field_ids_bytes, "`VARIANT` object field ids");
    ptr += field_ids_bytes;

    const UInt8 * field_offsets = ptr;
    size_t field_offsets_bytes = checkedVariantByteSizeWithSentinel(num_elements, field_offset_size, "`object field offsets`");
    ensureVariantRemaining(field_offsets, end, field_offsets_bytes, "`VARIANT` object field offsets");
    ptr += field_offsets_bytes;

    const UInt8 * values = ptr;
    const UInt8 * field_ids_ptr = field_ids;
    const UInt8 * offsets_ptr = field_offsets;
    UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");

    if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - values))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child offset");

    const auto & step = path.steps[step_idx];
    const bool has_nested_path = hasNestedPathComponent(path.parsed_path, step_idx);
    const bool can_match_exact = step.exact_match_field_id.has_value();
    const bool can_match_nested = has_nested_path && step.nested_key_field_id.has_value();

    const UInt8 * nested_begin = nullptr;
    const UInt8 * nested_end = nullptr;
    const UInt8 * exact_begin = nullptr;
    const UInt8 * exact_end = nullptr;

    std::unordered_set<UInt64> seen_field_ids;
    seen_field_ids.reserve(static_cast<size_t>(num_elements));
    for (UInt64 i = 0; i < num_elements; ++i)
    {
        UInt64 field_id = readVariantLittleEndianVariable(field_ids_ptr, field_ids + field_ids_bytes, field_id_size, "`VARIANT` object field id");
        UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");
        if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values) || field_id >= metadata.strings.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child metadata");
        checkUniqueObjectFieldId(seen_field_ids, field_id);

        const UInt8 * child_begin = values + previous_offset;
        const UInt8 * child_end = values + next_offset;

        if (!exact_begin && can_match_exact && field_id == *step.exact_match_field_id)
        {
            exact_begin = child_begin;
            exact_end = child_end;
        }

        if (!nested_begin && can_match_nested && field_id == *step.nested_key_field_id)
        {
            nested_begin = child_begin;
            nested_end = child_end;
        }

        previous_offset = next_offset;
    }

    checkVariantCollectionPayloadSize(previous_offset, values, end, "`object`");

    if (exact_begin)
        return tryDecodeValueByPathImpl(metadata, exact_begin, exact_end, path, result, format_settings, depth + 1, path.steps.size());

    if (nested_begin)
        return tryDecodeValueByPathImpl(metadata, nested_begin, nested_end, path, result, format_settings, depth + 1, step_idx + 1);

    return false;
}

enum class ScalarPathDecodeStatus
{
    Missing,
    Null,
    Exact,
    Unsupported,
};

static ScalarPathDecodeStatus tryDecodeScalarExactValueByPathImpl(
    const VariantMetadata & metadata,
    const UInt8 * begin,
    const UInt8 * end,
    const ResolvedVariantPath & path,
    ScalarExactValue & result,
    const FormatSettings & format_settings,
    size_t depth,
    size_t step_idx)
{
    checkVariantReadDepth(format_settings, depth);

    if (step_idx >= path.steps.size())
    {
        const UInt8 * ptr = begin;
        auto decode_status = tryDecodeScalarExactValueImpl(ptr, end, result, format_settings);
        if (decode_status == ScalarExactDecodeStatus::Unsupported)
            return ScalarPathDecodeStatus::Unsupported;
        if (ptr != end)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid child size while decoding scalar path");
        return decode_status == ScalarExactDecodeStatus::Null ? ScalarPathDecodeStatus::Null : ScalarPathDecodeStatus::Exact;
    }

    ensureVariantRemaining(begin, end, 1, "`VARIANT` value header");
    const UInt8 * ptr = begin;
    UInt8 header = *ptr++;

    VariantBasicType basic_type = decodeVariantBasicType(header);
    UInt8 value_header = header >> VARIANT_VALUE_HEADER_SHIFT;

    if (basic_type == VariantBasicType::Array)
        return ScalarPathDecodeStatus::Unsupported;

    if (basic_type != VariantBasicType::Object)
    {
        const UInt8 * value_ptr = begin;
        decodeValueImpl(metadata, value_ptr, end, format_settings, depth);
        if (value_ptr != end)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid value size while decoding scalar path");
        return ScalarPathDecodeStatus::Missing;
    }

    size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
    size_t field_id_size = ((value_header >> VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT) & VARIANT_FIELD_ID_SIZE_MINUS_ONE_MASK) + 1;
    bool is_large = ((value_header >> VARIANT_OBJECT_IS_LARGE_SHIFT) & 0x01) != 0;
    UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` object field count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` object field count");
    checkVariantObjectFieldCount(format_settings, num_elements);

    const UInt8 * field_ids = ptr;
    size_t field_ids_bytes = checkedVariantByteSize(num_elements, field_id_size, "`object field ids`");
    ensureVariantRemaining(field_ids, end, field_ids_bytes, "`VARIANT` object field ids");
    ptr += field_ids_bytes;

    const UInt8 * field_offsets = ptr;
    size_t field_offsets_bytes = checkedVariantByteSizeWithSentinel(num_elements, field_offset_size, "`object field offsets`");
    ensureVariantRemaining(field_offsets, end, field_offsets_bytes, "`VARIANT` object field offsets");
    ptr += field_offsets_bytes;

    const UInt8 * values = ptr;
    const UInt8 * field_ids_ptr = field_ids;
    const UInt8 * offsets_ptr = field_offsets;
    UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");

    if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - values))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child offset");

    const auto & step = path.steps[step_idx];
    const bool has_nested_path = hasNestedPathComponent(path.parsed_path, step_idx);
    const bool can_match_exact = step.exact_match_field_id.has_value();
    const bool can_match_nested = has_nested_path && step.nested_key_field_id.has_value();

    const UInt8 * nested_begin = nullptr;
    const UInt8 * nested_end = nullptr;
    const UInt8 * exact_begin = nullptr;
    const UInt8 * exact_end = nullptr;

    std::unordered_set<UInt64> seen_field_ids;
    seen_field_ids.reserve(static_cast<size_t>(num_elements));
    for (UInt64 i = 0; i < num_elements; ++i)
    {
        UInt64 field_id = readVariantLittleEndianVariable(field_ids_ptr, field_ids + field_ids_bytes, field_id_size, "`VARIANT` object field id");
        UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");
        if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values) || field_id >= metadata.strings.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child metadata");
        checkUniqueObjectFieldId(seen_field_ids, field_id);

        const UInt8 * child_begin = values + previous_offset;
        const UInt8 * child_end = values + next_offset;

        if (!exact_begin && can_match_exact && field_id == *step.exact_match_field_id)
        {
            exact_begin = child_begin;
            exact_end = child_end;
        }

        if (!nested_begin && can_match_nested && field_id == *step.nested_key_field_id)
        {
            nested_begin = child_begin;
            nested_end = child_end;
        }

        previous_offset = next_offset;
    }

    checkVariantCollectionPayloadSize(previous_offset, values, end, "`object`");

    if (exact_begin)
        return tryDecodeScalarExactValueByPathImpl(metadata, exact_begin, exact_end, path, result, format_settings, depth + 1, path.steps.size());

    if (nested_begin)
        return tryDecodeScalarExactValueByPathImpl(metadata, nested_begin, nested_end, path, result, format_settings, depth + 1, step_idx + 1);

    return ScalarPathDecodeStatus::Missing;
}

bool tryDecodeValueByPath(const VariantMetadata & metadata, std::string_view value_blob, std::string_view path, VariantValue & result, const FormatSettings & format_settings, size_t depth)
{
    return tryDecodeValueByPath(metadata, value_blob, parseVariantPath(path), result, format_settings, depth);
}

bool tryDecodeValueByPath(const VariantMetadata & metadata, std::string_view value_blob, const ParsedVariantPath & path, VariantValue & result, const FormatSettings & format_settings, size_t depth)
{
    checkVariantPayloadSize(format_settings, value_blob.size(), "`value` blob");
    const auto * begin = reinterpret_cast<const UInt8 *>(value_blob.data());
    const auto * end = begin + value_blob.size();
    return tryDecodeValueByPathImpl(metadata, begin, end, path, result, format_settings, depth, 0);
}

bool tryDecodeValueByPath(const VariantMetadata & metadata, std::string_view value_blob, const ResolvedVariantPath & path, VariantValue & result, const FormatSettings & format_settings, size_t depth)
{
    checkVariantPayloadSize(format_settings, value_blob.size(), "`value` blob");
    const auto * begin = reinterpret_cast<const UInt8 *>(value_blob.data());
    const auto * end = begin + value_blob.size();
    return tryDecodeValueByPathImpl(metadata, begin, end, path, result, format_settings, depth, 0);
}

ScalarExactPathStatus tryDecodeScalarExactValue(
    std::string_view value_blob,
    ScalarExactValue & result,
    const FormatSettings & format_settings,
    size_t depth)
{
    checkVariantReadDepth(format_settings, depth);
    checkVariantPayloadSize(format_settings, value_blob.size(), "`value` blob");
    const auto * begin = reinterpret_cast<const UInt8 *>(value_blob.data());
    const auto * end = begin + value_blob.size();
    const UInt8 * ptr = begin;

    auto status = tryDecodeScalarExactValueImpl(ptr, end, result, format_settings);
    if (status == ScalarExactDecodeStatus::Unsupported)
        return ScalarExactPathStatus::Unsupported;
    if (ptr != end)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid scalar child size");

    return status == ScalarExactDecodeStatus::Null ? ScalarExactPathStatus::Null : ScalarExactPathStatus::Exact;
}

void collectObjectFieldSlicesByResolvedIds(
    const VariantMetadata & metadata,
    std::string_view value_blob,
    const std::vector<UInt64> & field_ids,
    std::vector<std::optional<std::string_view>> & result_slices,
    const FormatSettings & format_settings,
    size_t depth)
{
    checkVariantReadDepth(format_settings, depth);
    result_slices.assign(field_ids.size(), std::nullopt);
    if (field_ids.empty())
        return;

    checkVariantPayloadSize(format_settings, value_blob.size(), "`value` blob");
    const auto * begin = reinterpret_cast<const UInt8 *>(value_blob.data());
    const auto * end = begin + value_blob.size();

    ensureVariantRemaining(begin, end, 1, "`VARIANT` value header");
    const UInt8 * ptr = begin;
    UInt8 header = *ptr++;

    VariantBasicType basic_type = decodeVariantBasicType(header);
    UInt8 value_header = header >> VARIANT_VALUE_HEADER_SHIFT;

    if (basic_type != VariantBasicType::Object)
    {
        const UInt8 * value_ptr = begin;
        decodeValueImpl(metadata, value_ptr, end, format_settings, depth);
        if (value_ptr != end)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid value size while collecting object field slices");
        return;
    }

    size_t field_offset_size = (value_header & VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
    size_t field_id_size = ((value_header >> VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT) & VARIANT_FIELD_ID_SIZE_MINUS_ONE_MASK) + 1;
    bool is_large = ((value_header >> VARIANT_OBJECT_IS_LARGE_SHIFT) & 0x01) != 0;
    UInt64 num_elements = is_large ? readVariantPOD<UInt32>(ptr, end, "`VARIANT` object field count") : readVariantPOD<UInt8>(ptr, end, "`VARIANT` object field count");
    checkVariantObjectFieldCount(format_settings, num_elements);

    const UInt8 * field_ids_data = ptr;
    size_t field_ids_bytes = checkedVariantByteSize(num_elements, field_id_size, "`object field ids`");
    ensureVariantRemaining(field_ids_data, end, field_ids_bytes, "`VARIANT` object field ids");
    ptr += field_ids_bytes;

    const UInt8 * field_offsets = ptr;
    size_t field_offsets_bytes = checkedVariantByteSizeWithSentinel(num_elements, field_offset_size, "`object field offsets`");
    ensureVariantRemaining(field_offsets, end, field_offsets_bytes, "`VARIANT` object field offsets");
    ptr += field_offsets_bytes;

    const UInt8 * values = ptr;
    const UInt8 * field_ids_ptr = field_ids_data;
    const UInt8 * offsets_ptr = field_offsets;
    UInt64 previous_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");

    if (previous_offset != 0 || previous_offset > static_cast<UInt64>(end - values))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child offset");

    std::unordered_set<UInt64> seen_field_ids;
    seen_field_ids.reserve(static_cast<size_t>(num_elements));
    for (UInt64 i = 0; i < num_elements; ++i)
    {
        UInt64 field_id = readVariantLittleEndianVariable(field_ids_ptr, field_ids_data + field_ids_bytes, field_id_size, "`VARIANT` object field id");
        UInt64 next_offset = readVariantLittleEndianVariable(offsets_ptr, field_offsets + field_offsets_bytes, field_offset_size, "`VARIANT` object field offset");
        if (next_offset < previous_offset || next_offset > static_cast<UInt64>(end - values) || field_id >= metadata.strings.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: invalid object child metadata");
        checkUniqueObjectFieldId(seen_field_ids, field_id);

        for (size_t requested_idx = 0; requested_idx < field_ids.size(); ++requested_idx)
        {
            if (result_slices[requested_idx].has_value() || field_ids[requested_idx] != field_id)
                continue;

            result_slices[requested_idx] = std::string_view(
                reinterpret_cast<const char *>(values + previous_offset),
                static_cast<size_t>(next_offset - previous_offset));
        }

        previous_offset = next_offset;
    }

    checkVariantCollectionPayloadSize(previous_offset, values, end, "`object`");
}

ScalarExactPathStatus tryDecodeScalarExactValueByPath(
    const VariantMetadata & metadata,
    std::string_view value_blob,
    const ResolvedVariantPath & path,
    ScalarExactValue & result,
    const FormatSettings & format_settings,
    size_t depth)
{
    checkVariantPayloadSize(format_settings, value_blob.size(), "`value` blob");
    const auto * begin = reinterpret_cast<const UInt8 *>(value_blob.data());
    const auto * end = begin + value_blob.size();
    auto status = tryDecodeScalarExactValueByPathImpl(metadata, begin, end, path, result, format_settings, depth, 0);
    switch (status)
    {
        case ScalarPathDecodeStatus::Missing:
            return ScalarExactPathStatus::Missing;
        case ScalarPathDecodeStatus::Null:
            return ScalarExactPathStatus::Null;
        case ScalarPathDecodeStatus::Exact:
            return ScalarExactPathStatus::Exact;
        case ScalarPathDecodeStatus::Unsupported:
            return ScalarExactPathStatus::Unsupported;
    }

    UNREACHABLE();
}


const VariantValue * tryGetSubcolumnValue(const VariantValue & value, std::string_view path, const FormatSettings & format_settings, size_t depth)
{
    return tryGetSubcolumnValue(value, parseVariantPath(path), format_settings, depth);
}

const VariantValue * tryGetSubcolumnValue(const VariantValue & value, const ParsedVariantPath & path, const FormatSettings & format_settings, size_t depth)
{
    return tryGetSubcolumnValueImpl(value, path, format_settings, depth, 0);
}

std::optional<VariantValue> extractSubcolumnValue(const VariantValue & value, const ParsedVariantPath & path, const FormatSettings & format_settings, size_t depth)
{
    return extractSubcolumnValueImpl(value, path, format_settings, depth, 0);
}

void fillVariantValueJSON(const VariantValue & value, String & out, const FormatSettings & format_settings)
{
    out.clear();
    WriteBufferFromString buffer(out);
    VariantValue json_value = value;
    if (auto nested_value = tryNestVariantJSONPaths(std::move(json_value), format_settings, 1))
        writeVariantValueAsJSON(*nested_value, buffer, format_settings, 1);
    else
        writeVariantValueAsJSON(value, buffer, format_settings, 1);
}

MutableColumnPtr materializeExactValueColumn(const VariantValue & value, const FormatSettings & format_settings)
{
    if (!value.exact_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot materialize `Parquet` `VARIANT` value without exact type");

    if (value.isExactValue())
        return materializeSingleExactValueColumn(value);

    return materializeVariantValueColumnImpl(value, value.exact_type, format_settings, 1);
}

void insertValueIntoOutput(
    IColumn & output_column,
    const DataTypePtr & output_type,
    const VariantValue & value,
    const FormatSettings & format_settings)
{
    insertVariantValueIntoTypedColumn(output_column, output_type, value, format_settings, 1);
}

}
