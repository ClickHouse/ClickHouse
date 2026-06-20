#include <Processors/Formats/Impl/Parquet/VariantWrite.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>
#include <Processors/Formats/Impl/Parquet/UUIDUtils.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <cmath>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_DEEP_RECURSION;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace DB::Parquet
{

namespace
{

struct VariantEncodingContext
{
    String metadata;
    std::unordered_map<String, UInt32> dictionary;
};


const DataTypePtr & getVariantBoolType()
{
    static const DataTypePtr type = DataTypeFactory::instance().get("Bool");
    return type;
}

const DataTypePtr & getVariantInt64Type()
{
    static const DataTypePtr type = std::make_shared<DataTypeInt64>();
    return type;
}

const DataTypePtr & getVariantUInt64Type()
{
    static const DataTypePtr type = std::make_shared<DataTypeUInt64>();
    return type;
}

const DataTypePtr & getVariantFloat64Type()
{
    static const DataTypePtr type = std::make_shared<DataTypeFloat64>();
    return type;
}

const DataTypePtr & getVariantStringType()
{
    static const DataTypePtr type = std::make_shared<DataTypeString>();
    return type;
}

void addVariantEncodingSizeOrThrow(size_t & total, size_t value, std::string_view what)
{
    if (common::addOverflow(total, value, total))
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Cannot encode `Parquet` `VARIANT` {} that exceeds supported size limits",
            what);
    }
}

void addVariantEncodingUInt64OrThrow(UInt64 & total, UInt64 value, std::string_view what)
{
    if (common::addOverflow(total, value, total))
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Cannot encode `Parquet` `VARIANT` {} that exceeds supported size limits",
            what);
    }
}

size_t multiplyVariantEncodingSizeOrThrow(size_t lhs, size_t rhs, std::string_view what)
{
    size_t result = 0;
    if (common::mulOverflow(lhs, rhs, result))
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Cannot encode `Parquet` `VARIANT` {} that exceeds supported size limits",
            what);
    }

    return result;
}

size_t addVariantEncodingSizesOrThrow(size_t lhs, size_t rhs, std::string_view what)
{
    addVariantEncodingSizeOrThrow(lhs, rhs, what);
    return lhs;
}

void addVariantAnalyzeScalarType(VariantWriteAnalysisNode & node, const DataTypePtr & type);
DataTypePtr getVariantAnalyzeScalarType(const Field & field, const DataTypePtr & type_hint);

void checkVariantWriteDepth(const FormatSettings & format_settings, size_t depth)
{
    if (depth > format_settings.max_parser_depth)
    {
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Maximum parse depth ({}) exceeded while encoding `Parquet` `VARIANT`. Consider raising `max_parser_depth` setting.",
            format_settings.max_parser_depth);
    }
}

const SerializationPtr & getVariantDynamicSerialization()
{
    static thread_local const SerializationPtr serialization = DataTypeDynamic().getDefaultSerialization();
    return serialization;
}

Field deserializeVariantObjectSharedDataValue(const ColumnString * shared_data_values, size_t index)
{
    auto value_data = shared_data_values->getDataAt(index);
    ReadBufferFromMemory buf(value_data);
    Field value;
    getVariantDynamicSerialization()->deserializeBinary(value, buf, FormatSettings());
    return value;
}

void collectVariantObjectKeysFromField(
    const Field & field,
    std::unordered_set<String> & unique_keys,
    const FormatSettings & format_settings,
    size_t depth)
{
    checkVariantWriteDepth(format_settings, depth);

    switch (field.getType())
    {
        case Field::Types::Object:
        {
            const auto & object = field.safeGet<Object>();
            for (const auto & [key, child] : object)
            {
                unique_keys.emplace(key);
                collectVariantObjectKeysFromField(child, unique_keys, format_settings, depth + 1);
            }
            return;
        }
        case Field::Types::Array:
        {
            const auto & array = field.safeGet<Array>();
            for (const auto & child : array)
                collectVariantObjectKeysFromField(child, unique_keys, format_settings, depth + 1);
            return;
        }
        case Field::Types::Tuple:
        {
            const auto & tuple = field.safeGet<Tuple>();
            for (const auto & child : tuple)
                collectVariantObjectKeysFromField(child, unique_keys, format_settings, depth + 1);
            return;
        }
        case Field::Types::Map:
        {
            const auto & map = field.safeGet<Map>();
            for (const auto & entry : map)
            {
                if (entry.getType() != Field::Types::Tuple)
                    continue;

                const auto & tuple = entry.safeGet<Tuple>();
                if (tuple.size() != 2 || tuple[0].getType() != Field::Types::String)
                    continue;

                unique_keys.emplace(tuple[0].safeGet<String>());
                collectVariantObjectKeysFromField(tuple[1], unique_keys, format_settings, depth + 1);
            }
            return;
        }
        default:
            return;
    }
}

using VariantFlatPathKeysCache = std::unordered_map<String, std::vector<String>>;

void collectVariantObjectKeysFromFlatPath(
    std::string_view path,
    std::unordered_set<String> & unique_keys,
    VariantFlatPathKeysCache * cache = nullptr)
{
    if (cache)
    {
        auto [it, inserted] = cache->try_emplace(String(path));
        if (inserted)
        {
            std::string_view current_path = it->first;
            while (!current_path.empty())
            {
                auto [head, tail] = Nested::splitName(current_path);
                it->second.emplace_back(unescapeDotInJSONKey(String(head)));
                current_path = tail;
            }
        }

        for (const auto & key : it->second)
            unique_keys.emplace(key);
        return;
    }

    while (!path.empty())
    {
        auto [head, tail] = Nested::splitName(path);
        unique_keys.emplace(unescapeDotInJSONKey(String(head)));
        path = tail;
    }
}

bool variantSharedDataTypeMayContainObjectKeys(const DataTypePtr & type)
{
    DataTypePtr nested_type = unwrapVariantTypeHint(type);
    if (!nested_type)
        return false;

    if (typeid_cast<const DataTypeObject *>(nested_type.get()) || typeid_cast<const DataTypeMap *>(nested_type.get()))
        return true;

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(nested_type.get()))
        return variantSharedDataTypeMayContainObjectKeys(array_type->getNestedType());

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_type.get()))
    {
        for (const auto & element : tuple_type->getElements())
        {
            if (variantSharedDataTypeMayContainObjectKeys(element))
                return true;
        }
    }

    return typeid_cast<const DataTypeDynamic *>(nested_type.get()) || nested_type->getTypeId() == TypeIndex::Variant;
}

bool variantSharedDataValueMayContainObjectKeys(std::string_view value_data)
{
    ReadBufferFromMemory buf(value_data);
    return variantSharedDataTypeMayContainObjectKeys(decodeDataType(buf));
}

void collectVariantObjectKeysForWrite(
    const ColumnObject & object_column,
    std::unordered_set<String> & unique_keys,
    const FormatSettings & format_settings)
{
    VariantFlatPathKeysCache flat_path_keys_cache;

    for (const auto & [path, _] : object_column.getTypedPaths())
        collectVariantObjectKeysFromFlatPath(path, unique_keys, &flat_path_keys_cache);

    for (const auto & [path, column] : object_column.getDynamicPathsPtrs())
    {
        collectVariantObjectKeysFromFlatPath(path, unique_keys, &flat_path_keys_cache);
        for (size_t row = 0; row < object_column.size(); ++row)
        {
            if (!column->isNullAt(row))
                collectVariantObjectKeysFromField((*column)[row], unique_keys, format_settings, 1);
        }
    }

    const auto & shared_data_offsets = object_column.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
    for (size_t row = 0; row < object_column.size(); ++row)
    {
        size_t begin = row == 0 ? 0 : shared_data_offsets[row - 1];
        size_t end = shared_data_offsets[row];
        for (size_t i = begin; i != end; ++i)
        {
            collectVariantObjectKeysFromFlatPath(shared_data_paths->getDataAt(i), unique_keys, &flat_path_keys_cache);
            auto value_data = shared_data_values->getDataAt(i);
            if (variantSharedDataValueMayContainObjectKeys(value_data))
                collectVariantObjectKeysFromField(
                    deserializeVariantObjectSharedDataValue(shared_data_values, i),
                    unique_keys,
                    format_settings,
                    1);
        }
    }
}

DataTypePtr removeNullableForVariantWrite(const DataTypePtr & type)
{
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        return nullable->getNestedType();

    return type;
}

std::vector<String> splitVariantObjectPath(std::string_view path)
{
    std::vector<String> segments;
    while (!path.empty())
    {
        auto [head, tail] = Nested::splitName(path);
        segments.emplace_back(unescapeDotInJSONKey(String(head)));
        path = tail;
    }

    return segments;
}

size_t getVariantScalarEncodedSizeFromColumn(
    const IColumn & column, size_t row, const IDataType & value_type, bool from_dynamic, const DataTypePtr & type_hint);
void writeVariantScalarFromColumnToBuffer(
    const IColumn & column, size_t row, const IDataType & value_type, bool from_dynamic, const DataTypePtr & type_hint, char *& out);
size_t getVariantEncodedStringSize(std::string_view value);
void writeVariantStringPayloadToBuffer(std::string_view value, char *& out);
template <typename T>
void writeVariantPODToBuffer(T value, char *& out);
void writeVariantLittleEndianToBuffer(UInt64 value, UInt8 size, char *& out);

/// The columnar residual-object encoder (defined alongside the columnar cursor below). Encodes the
/// residual `value` blob for one `ColumnObject` row directly from its leaves, descending into
/// `prefix` first and excluding the shredded top-level keys (`excluded_fields`). Returns
/// `std::nullopt` when nothing remains in the residual. `dictionary` resolves key field ids.
std::optional<String> encodeVariantColumnarObjectResidualForRow(
    const ColumnObject & object_column,
    const DataTypeObject & object_data_type,
    size_t row,
    const std::vector<String> & prefix,
    const std::unordered_set<String> & excluded_fields,
    const std::unordered_map<String, UInt32> & dictionary,
    const FormatSettings & format_settings);

void finishVariantObjectEncodingHeader(
    size_t num_children,
    UInt32 highest_field_id,
    size_t total_children_size,
    UInt8 & field_id_size,
    UInt8 & field_offset_size,
    bool & is_large,
    size_t & total_size)
{
    field_id_size = variantByteLength(highest_field_id);
    field_offset_size = variantByteLength(total_children_size);
    if (field_id_size > 4 || field_offset_size > 4)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Cannot encode `Parquet` `VARIANT` object header requiring field id size {} and offset size {}; maximum supported header width is 4 bytes",
            static_cast<UInt32>(field_id_size),
            static_cast<UInt32>(field_offset_size));
    }

    if (num_children > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Cannot encode `Parquet` `VARIANT` object with more than {} fields", std::numeric_limits<UInt32>::max());

    is_large = num_children > std::numeric_limits<UInt8>::max();
    size_t size = 1;
    addVariantEncodingSizeOrThrow(size, is_large ? sizeof(UInt32) : sizeof(UInt8), "`object` header");
    addVariantEncodingSizeOrThrow(
        size,
        multiplyVariantEncodingSizeOrThrow(num_children, field_id_size, "`object` field id table"),
        "`object` total size");
    addVariantEncodingSizeOrThrow(
        size,
        multiplyVariantEncodingSizeOrThrow(
            addVariantEncodingSizesOrThrow(num_children, size_t{1}, "`object` offset table size"),
            field_offset_size,
            "`object` field offset table"),
        "`object` total size");
    addVariantEncodingSizeOrThrow(size, total_children_size, "`object` total size");
    total_size = size;
}

template <typename Child, typename WriteChild>
void writeVariantObjectHeaderAndChildren(
    const std::vector<Child> & children,
    UInt8 field_id_size,
    UInt8 field_offset_size,
    bool is_large,
    char *& out,
    WriteChild && write_child)
{
    UInt8 header = static_cast<UInt8>(VariantBasicType::Object);
    header |= static_cast<UInt8>(field_offset_size - 1) << VARIANT_VALUE_HEADER_SHIFT;
    header |= static_cast<UInt8>(field_id_size - 1) << (VARIANT_VALUE_HEADER_SHIFT + VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT);
    header |= static_cast<UInt8>(is_large) << (VARIANT_VALUE_HEADER_SHIFT + VARIANT_OBJECT_IS_LARGE_SHIFT);
    *out = static_cast<char>(header);
    ++out;

    if (is_large)
        writeVariantPODToBuffer(static_cast<UInt32>(children.size()), out);
    else
    {
        *out = static_cast<char>(static_cast<UInt8>(children.size()));
        ++out;
    }

    for (const auto & child : children)
        writeVariantLittleEndianToBuffer(child.field_id, field_id_size, out);

    UInt64 offset = 0;
    for (const auto & child : children)
    {
        writeVariantLittleEndianToBuffer(offset, field_offset_size, out);
        addVariantEncodingUInt64OrThrow(offset, static_cast<UInt64>(child.child_size), "`object` child offsets");
    }
    writeVariantLittleEndianToBuffer(offset, field_offset_size, out);

    for (const auto & child : children)
        write_child(child);
}

std::unordered_set<String> getVariantTupleFieldNames(const DataTypeTuple & tuple_type)
{
    std::unordered_set<String> field_names;
    field_names.reserve(tuple_type.getElements().size());
    for (size_t i = 0; i < tuple_type.getElements().size(); ++i)
        field_names.emplace(tuple_type.getNameByPosition(i + 1));

    return field_names;
}

MutableColumnPtr buildVariantResidualValueColumnForObject(
    const ColumnObject & object_column,
    const DataTypePtr & object_type,
    const std::vector<String> & prefix,
    const std::unordered_set<String> & excluded_fields,
    const FormatSettings & format_settings,
    const VariantEncodingContext & context)
{
    auto value_column = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    auto & nullable_value = assert_cast<ColumnNullable &>(*value_column);
    auto & nested_value = assert_cast<ColumnString &>(nullable_value.getNestedColumn());
    auto & null_map = nullable_value.getNullMapData();

    nested_value.reserve(object_column.size());
    null_map.reserve(object_column.size());

    const auto * object_data_type = typeid_cast<const DataTypeObject *>(object_type.get());
    chassert(object_data_type);

    for (size_t row = 0; row < object_column.size(); ++row)
    {
        /// Encode the residual `value` blob columnar from the object's leaves through the cursor,
        /// descending into `prefix` and excluding the shredded top-level keys.
        std::optional<String> encoded_value = encodeVariantColumnarObjectResidualForRow(
            object_column, *object_data_type, row, prefix, excluded_fields, context.dictionary, format_settings);

        if (!encoded_value.has_value())
        {
            nested_value.insertDefault();
            null_map.push_back(UInt8(1));
        }
        else
        {
            nested_value.insertData(encoded_value->data(), encoded_value->size());
            null_map.push_back(UInt8(0));
        }
    }

    return value_column;
}

ColumnPtr buildVariantObjectTypedValueColumnFast(
    const ColumnObject & object_column,
    const DataTypePtr & object_type,
    const DataTypePtr & typed_value_type,
    std::string_view path,
    const FormatSettings & format_settings,
    const VariantEncodingContext & context);

ColumnPtr buildVariantObjectWrapperColumnFast(
    const ColumnObject & object_column,
    const DataTypePtr & object_type,
    const DataTypePtr & wrapper_type,
    const std::vector<String> & prefix,
    std::string_view path,
    const FormatSettings & format_settings,
    const VariantEncodingContext & context)
{
    auto layout = tryGetVariantWrapperLayout(wrapper_type);
    chassert(layout && layout->typed_value_pos.has_value());

    const auto & wrapper_tuple_type = assert_cast<const DataTypeTuple &>(*removeNullableForVariantWrite(wrapper_type));
    Columns columns(wrapper_tuple_type.getElements().size());

    DataTypePtr typed_value_type = layout->typed_value_type;
    DataTypePtr typed_value_nested_type = removeNullableForVariantWrite(typed_value_type);
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(typed_value_nested_type.get()))
    {
        columns[layout->value_pos] = buildVariantResidualValueColumnForObject(
            object_column,
            object_type,
            prefix,
            getVariantTupleFieldNames(*tuple_type),
            format_settings,
            context);
        columns[*layout->typed_value_pos] = buildVariantObjectTypedValueColumnFast(object_column, object_type, typed_value_type, path, format_settings, context);
    }
    else
    {
        auto value_nested = ColumnString::create();
        value_nested->insertManyDefaults(object_column.size());
        columns[layout->value_pos] = ColumnNullable::create(std::move(value_nested), ColumnUInt8::create(object_column.size(), UInt8(1)));
        columns[*layout->typed_value_pos] = buildVariantObjectTypedValueColumnFast(object_column, object_type, typed_value_type, path, format_settings, context);
    }

    auto tuple_column = ColumnTuple::create(std::move(columns));
    return ColumnNullable::create(std::move(tuple_column), ColumnUInt8::create(object_column.size(), UInt8(0)));
}

ColumnPtr buildVariantObjectTypedValueColumnFast(
    const ColumnObject & object_column,
    const DataTypePtr & object_type,
    const DataTypePtr & typed_value_type,
    std::string_view path,
    const FormatSettings & format_settings,
    const VariantEncodingContext & context)
{
    DataTypePtr nested_type = removeNullableForVariantWrite(typed_value_type);
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_type.get()))
    {
        Columns columns;
        columns.reserve(tuple_type->getElements().size());
        std::vector<String> prefix = splitVariantObjectPath(path);

        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            const String & field_name = tuple_type->getNameByPosition(i + 1);
            String child_path = appendVariantJSONPath(path, field_name);
            std::vector<String> child_prefix = prefix;
            child_prefix.emplace_back(field_name);
            columns.emplace_back(buildVariantObjectWrapperColumnFast(
                object_column,
                object_type,
                tuple_type->getElement(i),
                child_prefix,
                child_path,
                format_settings,
                context));
        }

        auto tuple_column = ColumnTuple::create(std::move(columns));
        if (typeid_cast<const DataTypeNullable *>(typed_value_type.get()))
            return ColumnNullable::create(std::move(tuple_column), ColumnUInt8::create(object_column.size(), UInt8(0)));

        return tuple_column;
    }

    auto it = object_column.getTypedPaths().find(path);
    ColumnPtr nested_column;
    if (it != object_column.getTypedPaths().end())
    {
        nested_column = it->second->convertToFullColumnIfLowCardinality();
    }
    else
    {
        auto default_column = nested_type->createColumn();
        default_column->insertManyDefaults(object_column.size());
        nested_column = std::move(default_column);
    }

    if (typeid_cast<const DataTypeNullable *>(typed_value_type.get()))
        return ColumnNullable::create(nested_column, ColumnUInt8::create(object_column.size(), UInt8(0)));

    return nested_column;
}

std::optional<Field> tryConvertVariantScalarToShreddedField(const Field & field, const DataTypePtr & type);

ColumnPtr buildVariantMetadataColumn(size_t num_rows, std::string_view metadata)
{
    auto column = ColumnString::create();
    column->reserve(num_rows);
    size_t row_size = addVariantEncodingSizesOrThrow(metadata.size(), 1, "`metadata` row size");
    column->getChars().reserve(multiplyVariantEncodingSizeOrThrow(row_size, num_rows, "`metadata` column size"));
    for (size_t row = 0; row < num_rows; ++row)
        column->insertData(metadata.data(), metadata.size());
    return column;
}

bool variantObjectFastPathSupportsScalarType(const DataTypePtr & type)
{
    DataTypePtr nested_type = removeNullableForVariantWrite(type);
    if (!nested_type)
        return false;

    if (isBool(nested_type))
        return true;

    switch (nested_type->getTypeId())
    {
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::String:
        case TypeIndex::FixedString:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::UUID:
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
            return true;
        default:
            return false;
    }
}

bool variantObjectFastPathMatchesTypedValue(
    const DataTypeObject & object_type,
    const DataTypePtr & typed_value_type,
    std::string_view path);

bool variantObjectFastPathMatchesWrapper(
    const DataTypeObject & object_type,
    const DataTypePtr & wrapper_type,
    std::string_view path)
{
    auto layout = tryGetVariantWrapperLayout(wrapper_type);
    if (!layout || !layout->typed_value_pos.has_value())
        return false;

    return variantObjectFastPathMatchesTypedValue(object_type, layout->typed_value_type, path);
}

bool variantObjectFastPathMatchesTypedValue(
    const DataTypeObject & object_type,
    const DataTypePtr & typed_value_type,
    std::string_view path)
{
    DataTypePtr nested_type = removeNullableForVariantWrite(typed_value_type);
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_type.get()))
    {
        if (!tuple_type->hasExplicitNames())
            return false;

        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            String child_path = appendVariantJSONPath(path, tuple_type->getNameByPosition(i + 1));
            if (!variantObjectFastPathMatchesWrapper(object_type, tuple_type->getElement(i), child_path))
                return false;
        }

        return true;
    }

    if (!variantObjectFastPathSupportsScalarType(nested_type))
        return false;

    auto it = object_type.getTypedPaths().find(String(path));
    if (it == object_type.getTypedPaths().end())
        return false;

    /// The direct path wraps every source value as a present `typed_value`, so
    /// nullable source paths still need the generic row-wise transform.
    if (isNullableOrLowCardinalityNullable(it->second))
        return false;

    DataTypePtr source_type = unwrapVariantTypeHint(it->second);
    return source_type && source_type->equals(*nested_type);
}

VariantEncodingContext buildVariantEncodingContext(const std::unordered_set<String> & unique_keys);

std::optional<PreparedVariantColumns> tryPrepareObjectVariantColumnsFast(
    const IColumn & full_column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    const DataTypePtr & shredded_type)
{
    const auto * object_type = typeid_cast<const DataTypeObject *>(type.get());
    if (!object_type || object_type->getSchemaFormat() != DataTypeObject::SchemaFormat::JSON || !shredded_type)
        return std::nullopt;

    const auto * object_column = typeid_cast<const ColumnObject *>(&full_column);
    if (!object_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected `ColumnObject` while preparing `Parquet` `VARIANT` write columns");

    if (!typeid_cast<const DataTypeTuple *>(shredded_type.get()))
        return std::nullopt;

    if (!variantObjectFastPathMatchesTypedValue(*object_type, shredded_type, std::string_view{}))
        return std::nullopt;

    std::unordered_set<String> unique_keys;
    collectVariantObjectKeysForWrite(*object_column, unique_keys, format_settings);
    VariantEncodingContext shared_context = buildVariantEncodingContext(unique_keys);

    const size_t num_rows = full_column.size();
    PreparedVariantColumns result;
    result.metadata_type = getVariantStringType();
    result.metadata_column = buildVariantMetadataColumn(num_rows, shared_context.metadata);
    result.value_type = std::make_shared<DataTypeNullable>(getVariantStringType());
    result.typed_value_type = std::make_shared<DataTypeNullable>(shredded_type);

    const auto & tuple_type = assert_cast<const DataTypeTuple &>(*shredded_type);
    result.value_column = buildVariantResidualValueColumnForObject(
        *object_column,
        type,
        {},
        getVariantTupleFieldNames(tuple_type),
        format_settings,
        shared_context);
    result.typed_value_column = buildVariantObjectTypedValueColumnFast(
        *object_column,
        type,
        result.typed_value_type,
        std::string_view{},
        format_settings,
        shared_context);

    return result;
}

template <typename Target, typename Source>
bool tryConvertNativeIntegralValue(Source source, Target & value);

/// Range-checks and narrows an integral `Field` into `value`.
template <typename T>
bool tryConvertIntegralFieldValue(const Field & field, T & value)
{
    switch (field.getType())
    {
        case Field::Types::Int64:
            return tryConvertNativeIntegralValue(field.safeGet<Int64>(), value);
        case Field::Types::UInt64:
            return tryConvertNativeIntegralValue(field.safeGet<UInt64>(), value);
        case Field::Types::Int128:
            return tryConvertNativeIntegralValue(field.safeGet<Int128>(), value);
        case Field::Types::UInt128:
            return tryConvertNativeIntegralValue(field.safeGet<UInt128>(), value);
        case Field::Types::Int256:
            return tryConvertNativeIntegralValue(field.safeGet<Int256>(), value);
        case Field::Types::UInt256:
            return tryConvertNativeIntegralValue(field.safeGet<UInt256>(), value);
        default:
            return false;
    }
}

bool tryRescaleVariantTemporalValue(Int64 raw_value, UInt32 from_scale, UInt32 to_scale, Int64 & result)
{
    if (from_scale == to_scale)
    {
        result = raw_value;
        return true;
    }

    if (from_scale < to_scale)
    {
        Int64 multiplier = DecimalUtils::scaleMultiplier<Int64>(to_scale - from_scale);
        return !common::mulOverflow(raw_value, multiplier, result);
    }

    result = raw_value / DecimalUtils::scaleMultiplier<Int64>(from_scale - to_scale);
    return true;
}

template <typename Predicate>
std::optional<Field> tryNormalizeVariantTemporalScalarToString(
    const Field & field,
    const DataTypePtr & type_hint,
    const FormatSettings & format_settings,
    Predicate && should_normalize)
{
    DataTypePtr normalized_type = unwrapVariantTypeHint(type_hint);
    if (!normalized_type)
        return std::nullopt;

    if (!should_normalize(normalized_type))
        return std::nullopt;

    if (field.getType() == Field::Types::String)
        return field;

    auto tmp_column = normalized_type->createColumn();
    tmp_column->insert(field);

    WriteBufferFromOwnString wb;
    normalized_type->getDefaultSerialization()->serializeTextJSON(*tmp_column, 0, wb, format_settings);
    String json = wb.str();

    if (!json.empty() && json.front() == '"')
    {
        ReadBufferFromString rb(json);
        String decoded;
        readJSONString(decoded, rb, format_settings.json);
        return Field(std::move(decoded));
    }

    return Field(std::move(json));
}

std::optional<Field> tryConvertVariantScalarToShreddedField(const Field & field, const DataTypePtr & type)
{
    DataTypePtr normalized_type = unwrapVariantTypeHint(type);
    if (!normalized_type)
        return std::nullopt;

    if (isBool(normalized_type))
    {
        if (field.getType() != Field::Types::Bool)
            return std::nullopt;
        return field;
    }

    auto passthrough_if = [&](Field::Types::Which expected) -> std::optional<Field>
    {
        if (field.getType() != expected)
            return std::nullopt;
        return field;
    };

    switch (normalized_type->getTypeId())
    {
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Date32:
        {
            Int64 converted = 0;
            if (!tryConvertIntegralFieldValue(field, converted))
                return std::nullopt;
            return Field(converted);
        }
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::Date:
        case TypeIndex::DateTime:
        {
            UInt64 converted = 0;
            if (!tryConvertIntegralFieldValue(field, converted))
                return std::nullopt;
            return Field(converted);
        }
        case TypeIndex::Float32:
        case TypeIndex::Float64:
            return passthrough_if(Field::Types::Float64);
        case TypeIndex::String:
            return passthrough_if(Field::Types::String);
        case TypeIndex::FixedString:
            return passthrough_if(Field::Types::String);
        case TypeIndex::UUID:
            return passthrough_if(Field::Types::UUID);
        case TypeIndex::IPv4:
            return passthrough_if(Field::Types::IPv4);
        case TypeIndex::IPv6:
            return passthrough_if(Field::Types::IPv6);
        case TypeIndex::Decimal32:
            return passthrough_if(Field::Types::Decimal32);
        case TypeIndex::Decimal64:
        case TypeIndex::DateTime64:
            return passthrough_if(Field::Types::Decimal64);
        case TypeIndex::Decimal128:
            return passthrough_if(Field::Types::Decimal128);
        case TypeIndex::Decimal256:
            return passthrough_if(Field::Types::Decimal256);
        case TypeIndex::Int128:
            return passthrough_if(Field::Types::Int128);
        case TypeIndex::UInt128:
            return passthrough_if(Field::Types::UInt128);
        case TypeIndex::Int256:
            return passthrough_if(Field::Types::Int256);
        case TypeIndex::UInt256:
            return passthrough_if(Field::Types::UInt256);
        default:
            return std::nullopt;
    }
}


void addVariantAnalyzeScalarType(VariantWriteAnalysisNode & node, const DataTypePtr & type)
{
    auto & [count, stored_type] = node.scalar_types[type->getName()];
    ++count;
    if (!stored_type)
        stored_type = type;
}

DataTypePtr getVariantAnalyzeScalarType(const Field & field, const DataTypePtr & type_hint)
{
    DataTypePtr normalized_type_hint = unwrapVariantTypeHint(type_hint);
    if (normalized_type_hint && tryConvertVariantScalarToShreddedField(field, normalized_type_hint))
        return normalized_type_hint;

    switch (field.getType())
    {
        case Field::Types::Null:
            return nullptr;
        case Field::Types::Bool:
            return getVariantBoolType();
        case Field::Types::Int64:
        case Field::Types::Int128:
        case Field::Types::Int256:
            return getVariantInt64Type();
        case Field::Types::UInt64:
            return getVariantUInt64Type();
        case Field::Types::UInt128:
        case Field::Types::UInt256:
        case Field::Types::Float64:
            return getVariantFloat64Type();
        case Field::Types::String:
            return getVariantStringType();
        default:
            return nullptr;
    }
}

enum class VariantAnalyzeCandidate
{
    None,
    Scalar,
    Array,
    Object,
};

struct VariantAnalyzeChoice
{
    VariantAnalyzeCandidate candidate = VariantAnalyzeCandidate::None;
    size_t count = 0;
    bool is_ambiguous = false;
    DataTypePtr scalar_type;
};

VariantAnalyzeChoice chooseVariantAnalyzeCandidate(const VariantWriteAnalysisNode & node)
{
    VariantAnalyzeChoice choice;

    auto consider = [&](size_t count, VariantAnalyzeCandidate candidate)
    {
        if (count == 0)
            return;

        if (count > choice.count)
        {
            choice.count = count;
            choice.candidate = candidate;
            choice.is_ambiguous = false;
        }
        else if (count == choice.count)
        {
            choice.is_ambiguous = true;
        }
    };

    for (const auto & [_, scalar_type] : node.scalar_types)
    {
        if (scalar_type.first > choice.count)
        {
            choice.count = scalar_type.first;
            choice.candidate = VariantAnalyzeCandidate::Scalar;
            choice.scalar_type = scalar_type.second;
            choice.is_ambiguous = false;
        }
        else if (scalar_type.first == choice.count && choice.count != 0)
        {
            choice.is_ambiguous = true;
        }
    }

    consider(node.array_count, VariantAnalyzeCandidate::Array);
    consider(node.object_count, VariantAnalyzeCandidate::Object);

    return choice;
}

bool isParquetVariantWrapperFieldName(std::string_view name)
{
    return name == "value" || name == "typed_value";
}

void collectShreddedPathCandidates(
    const VariantWriteAnalysisNode & node,
    std::string_view current_path,
    std::vector<std::pair<size_t, String>> & out)
{
    VariantAnalyzeChoice choice = chooseVariantAnalyzeCandidate(node);
    if (choice.is_ambiguous)
        return;

    switch (choice.candidate)
    {
        case VariantAnalyzeCandidate::Scalar:
        case VariantAnalyzeCandidate::Array:
            if (!current_path.empty())
                out.emplace_back(choice.count, String(current_path));
            return;
        case VariantAnalyzeCandidate::Object:
            for (const auto & [name, child_node] : node.object_fields)
            {
                if (isParquetVariantWrapperFieldName(name))
                    continue;

                String child_path = appendVariantJSONPath(current_path, name);
                collectShreddedPathCandidates(child_node, child_path, out);
            }
            return;
        case VariantAnalyzeCandidate::None:
            return;
    }
}

std::unordered_set<String> selectShreddedPathsForObject(
    const VariantWriteAnalysisNode & analysis,
    const DataTypeObject & object_type)
{
    std::unordered_set<String> selected_paths;
    for (const auto & [path, _] : object_type.getTypedPaths())
        selected_paths.emplace(path);

    std::vector<std::pair<size_t, String>> candidates;
    collectShreddedPathCandidates(analysis, std::string_view{}, candidates);

    std::sort(candidates.begin(), candidates.end(), [](const auto & left, const auto & right)
    {
        return std::tuple(right.first, left.second) < std::tuple(left.first, right.second);
    });

    size_t selected_dynamic_paths = 0;
    for (const auto & [_, path] : candidates)
    {
        if (selected_paths.contains(path))
            continue;

        if (selected_dynamic_paths >= object_type.getMaxDynamicPaths())
            break;

        selected_paths.emplace(path);
        ++selected_dynamic_paths;
    }

    return selected_paths;
}

DataTypePtr constructShreddedType(
    const VariantWriteAnalysisNode & node,
    const std::unordered_set<String> * selected_paths = nullptr,
    std::string_view current_path = {},
    bool array_needs_full_coverage = false)
{
    VariantAnalyzeChoice choice = chooseVariantAnalyzeCandidate(node);

    if (choice.is_ambiguous)
        return nullptr;

    const bool current_path_selected = selected_paths && !current_path.empty() && selected_paths->contains(String(current_path));

    switch (choice.candidate)
    {
        case VariantAnalyzeCandidate::Scalar:
            if (selected_paths && !current_path_selected)
                return nullptr;
            return choice.scalar_type;
        case VariantAnalyzeCandidate::Array:
        {
            if (current_path.empty() || (selected_paths && !current_path_selected))
                return nullptr;

            if (array_needs_full_coverage && node.array_count != node.value_count)
                return nullptr;

            if (!node.array_child)
                return nullptr;

            DataTypePtr child = constructShreddedType(*node.array_child, nullptr, current_path, true);
            if (!child)
                return nullptr;

            return std::make_shared<DataTypeArray>(makeVariantWrappedTypedValueType(child));
        }
        case VariantAnalyzeCandidate::Object:
        {
            DataTypes field_types;
            Names field_names;

            for (const auto & [name, child_node] : node.object_fields)
            {
                if (isParquetVariantWrapperFieldName(name))
                    continue;

                String child_path = appendVariantJSONPath(current_path, name);
                DataTypePtr child = constructShreddedType(
                    child_node,
                    current_path_selected ? nullptr : selected_paths,
                    child_path,
                    true);
                if (!child)
                    continue;

                field_names.push_back(name);
                field_types.push_back(makeVariantWrappedTypedValueType(child));
            }

            if (field_types.empty())
                return nullptr;

            return std::make_shared<DataTypeTuple>(field_types, field_names);
        }
        case VariantAnalyzeCandidate::None:
            return nullptr;
    }

    return nullptr;
}

template <typename T>
void writeVariantPODToBuffer(T value, char *& out)
{
    transformEndianness<std::endian::little>(value);
    memcpy(out, &value, sizeof(T));
    out += sizeof(T);
}

void writeVariantLittleEndianToBuffer(UInt64 value, UInt8 size, char *& out)
{
    for (size_t i = 0; i < size; ++i)
    {
        *out = static_cast<char>((value >> (8 * i)) & 0xFF);
        ++out;
    }
}

size_t getVariantEncodedStringSize(std::string_view value)
{
    return value.size() <= 63 ? 1 + value.size() : 1 + sizeof(UInt32) + value.size();
}

void writeVariantStringPayloadToBuffer(std::string_view value, char *& out)
{
    if (value.size() <= 63)
    {
        UInt8 header = static_cast<UInt8>(static_cast<UInt8>(VariantBasicType::ShortString) | (static_cast<UInt8>(value.size()) << VARIANT_VALUE_HEADER_SHIFT));
        *out = static_cast<char>(header);
        ++out;
        memcpy(out, value.data(), value.size());
        out += value.size();
        return;
    }

    if (value.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Cannot encode `Parquet` `VARIANT` string larger than 4 GiB");

    *out = static_cast<char>(static_cast<UInt8>(VariantBasicType::Primitive) | (static_cast<UInt8>(VariantPrimitiveType::String) << VARIANT_VALUE_HEADER_SHIFT));
    ++out;
    writeVariantPODToBuffer(static_cast<UInt32>(value.size()), out);
    memcpy(out, value.data(), value.size());
    out += value.size();
}

struct VariantScalarMeasureSink
{
    size_t size = 0;

    void writePrimitiveHeader(VariantPrimitiveType)
    {
        ++size;
    }

    template <typename T>
    void writePOD(T)
    {
        size += sizeof(T);
    }

    void writeUUID(UUID)
    {
        size += sizeof(UUID);
    }

    void writeString(std::string_view value)
    {
        size += getVariantEncodedStringSize(value);
    }
};

struct VariantScalarWriteSink
{
    char *& out;

    explicit VariantScalarWriteSink(char *& out_)
        : out(out_)
    {
    }

    void writePrimitiveHeader(VariantPrimitiveType type)
    {
        *out = static_cast<char>(static_cast<UInt8>(VariantBasicType::Primitive) | (static_cast<UInt8>(type) << VARIANT_VALUE_HEADER_SHIFT));
        ++out;
    }

    template <typename T>
    void writePOD(T value)
    {
        writeVariantPODToBuffer(value, out);
    }

    void writeUUID(UUID value)
    {
        UUID parquet_value = encodeParquetUUID(value);
        memcpy(out, &parquet_value, sizeof(parquet_value));
        out += sizeof(parquet_value);
    }

    void writeString(std::string_view value)
    {
        writeVariantStringPayloadToBuffer(value, out);
    }
};

template <typename Sink, typename T>
void writeVariantSignedIntegralPrimitive(VariantPrimitiveType type, T value, Sink & sink)
{
    sink.writePrimitiveHeader(type);
    sink.writePOD(value);
}

/// Native twin of `tryConvertIntegralFieldValue`. `Source` is the `NearestFieldType` representation
/// of the source column value (`Int64`/`UInt64`/`Int128`/`UInt128`/`Int256`/`UInt256`), so the
/// branch selected here matches the corresponding `Field`-type case in `tryConvertIntegralFieldValue`.
template <typename Target, typename Source>
bool tryConvertNativeIntegralValue(Source source, Target & value)
{
    if constexpr (std::numeric_limits<Source>::is_signed)
    {
        if constexpr (std::numeric_limits<Target>::is_signed)
        {
            if (source < static_cast<Source>(std::numeric_limits<Target>::min())
                || source > static_cast<Source>(std::numeric_limits<Target>::max()))
                return false;
        }
        else
        {
            using UnsignedSource = ::make_unsigned_t<Source>;
            if (source < 0 || static_cast<UnsignedSource>(source) > static_cast<UnsignedSource>(std::numeric_limits<Target>::max()))
                return false;
        }
    }
    else
    {
        if (source > static_cast<Source>(std::numeric_limits<Target>::max()))
            return false;
    }

    value = static_cast<Target>(source);
    return true;
}

/// Reads the column's element value and returns it widened to its `NearestFieldType` integral
/// representation, mirroring the value that `(*column)[row]` would store inside a `Field`.
template <typename ColumnElement>
auto readColumnNearestFieldIntegral(const IColumn & column, size_t row)
{
    using FieldType = NearestFieldType<ColumnElement>;
    return static_cast<FieldType>(assert_cast<const ColumnVector<ColumnElement> &>(column).getData()[row]);
}

template <typename Target>
bool tryReadColumnIntegralValue(const IColumn & column, size_t row, const IDataType & value_type, Target & value)
{
    switch (value_type.getTypeId())
    {
        case TypeIndex::Int8:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<Int8>(column, row), value);
        case TypeIndex::Int16:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<Int16>(column, row), value);
        case TypeIndex::Int32:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<Int32>(column, row), value);
        case TypeIndex::Date32:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<Int32>(column, row), value);
        case TypeIndex::Time:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<Int32>(column, row), value);
        case TypeIndex::Int64:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<Int64>(column, row), value);
        case TypeIndex::UInt8:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<UInt8>(column, row), value);
        case TypeIndex::UInt16:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<UInt16>(column, row), value);
        case TypeIndex::Date:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<UInt16>(column, row), value);
        case TypeIndex::UInt32:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<UInt32>(column, row), value);
        case TypeIndex::DateTime:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<UInt32>(column, row), value);
        case TypeIndex::UInt64:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<UInt64>(column, row), value);
        case TypeIndex::Int128:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<Int128>(column, row), value);
        case TypeIndex::UInt128:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<UInt128>(column, row), value);
        case TypeIndex::Int256:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<Int256>(column, row), value);
        case TypeIndex::UInt256:
            return tryConvertNativeIntegralValue(readColumnNearestFieldIntegral<UInt256>(column, row), value);
        default:
            return false;
    }
}

/// Type-hint stage of the columnar scalar encoder: reads the value from `(column, row)` and, when the
/// type hint pins a concrete temporal/integral/float `VARIANT` primitive, encodes it directly through
/// `sink`. Returns false when no hint applies, leaving the fallback dispatch to encode the value.
template <typename Sink>
bool tryEncodeVariantScalarFromColumnUsingTypeHint(
    const IColumn & column, size_t row, const IDataType & value_type, const DataTypePtr & type_hint, Sink & sink)
{
    if (!type_hint)
        return false;

    DataTypePtr normalized_type = unwrapVariantTypeHint(type_hint);
    if (!normalized_type)
        return false;

    switch (normalized_type->getTypeId())
    {
        case TypeIndex::Date:
        {
            UInt64 converted = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, converted)
                || converted > static_cast<UInt64>(std::numeric_limits<Int32>::max()))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Date, static_cast<Int32>(converted), sink);
            return true;
        }
        case TypeIndex::Date32:
        {
            Int32 converted = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, converted))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Date, converted, sink);
            return true;
        }
        case TypeIndex::DateTime:
        {
            UInt64 converted = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, converted)
                || converted > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return false;

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(static_cast<Int64>(converted), 0, 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `DateTime` value as `Parquet` `VARIANT` timestamp");

            const auto & date_time_type = assert_cast<const DataTypeDateTime &>(*normalized_type);
            writeVariantSignedIntegralPrimitive(
                date_time_type.hasExplicitTimeZone() ? VariantPrimitiveType::TimestampMicros : VariantPrimitiveType::TimestampNtzMicros,
                micros,
                sink);
            return true;
        }
        case TypeIndex::DateTime64:
        {
            if (value_type.getTypeId() != TypeIndex::DateTime64)
                return false;

            const auto & date_time_type = assert_cast<const DataTypeDateTime64 &>(*normalized_type);
            DateTime64 raw = assert_cast<const ColumnDecimal<DateTime64> &>(column).getData()[row];
            UInt32 target_scale = date_time_type.getScale() <= 6 ? 6 : 9;

            Int64 scaled = 0;
            if (!tryRescaleVariantTemporalValue(raw.value, date_time_type.getScale(), target_scale, scaled))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `DateTime64` value as `Parquet` `VARIANT` timestamp");

            const VariantPrimitiveType primitive_type = target_scale == 6
                ? (date_time_type.hasExplicitTimeZone() ? VariantPrimitiveType::TimestampMicros : VariantPrimitiveType::TimestampNtzMicros)
                : (date_time_type.hasExplicitTimeZone() ? VariantPrimitiveType::TimestampNanos : VariantPrimitiveType::TimestampNtzNanos);

            writeVariantSignedIntegralPrimitive(primitive_type, scaled, sink);
            return true;
        }
        case TypeIndex::Time:
        {
            Int64 converted = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, converted))
                return false;

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(converted, 0, 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `Time` value as `Parquet` `VARIANT` time");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::TimeNtzMicros, micros, sink);
            return true;
        }
        case TypeIndex::Time64:
        {
            if (value_type.getTypeId() != TypeIndex::Time64)
                return false;

            const auto & time_type = assert_cast<const DataTypeTime64 &>(*normalized_type);
            Time64 raw = assert_cast<const ColumnDecimal<Time64> &>(column).getData()[row];

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(raw.value, time_type.getScale(), 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `Time64` value as `Parquet` `VARIANT` time");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::TimeNtzMicros, micros, sink);
            return true;
        }
        case TypeIndex::Int8:
        {
            Int8 value = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int8, value, sink);
            return true;
        }
        case TypeIndex::Int16:
        {
            Int16 value = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int16, value, sink);
            return true;
        }
        case TypeIndex::Int32:
        {
            Int32 value = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int32, value, sink);
            return true;
        }
        case TypeIndex::UInt8:
        {
            Int16 value = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int16, value, sink);
            return true;
        }
        case TypeIndex::UInt16:
        {
            Int32 value = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int32, value, sink);
            return true;
        }
        case TypeIndex::UInt32:
        {
            Int64 value = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, value, sink);
            return true;
        }
        case TypeIndex::Float32:
        {
            /// Both `Float32` and `Float64` columns are accepted here because both widen to a `Float64`
            /// value (`NearestFieldType<Float32> == Float64`).
            Float64 value;
            if (value_type.getTypeId() == TypeIndex::Float32)
                value = static_cast<Float64>(assert_cast<const ColumnVector<Float32> &>(column).getData()[row]);
            else if (value_type.getTypeId() == TypeIndex::Float64)
                value = assert_cast<const ColumnVector<Float64> &>(column).getData()[row];
            else
                return false;

            if (!std::isfinite(value))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode non-finite `Parquet` `VARIANT` `FLOAT`");

            sink.writePrimitiveHeader(VariantPrimitiveType::Float);
            sink.writePOD(static_cast<Float32>(value));
            return true;
        }
        default:
            return false;
    }
}

template <typename T, typename Sink>
void writeVariantDecimalFromColumn(VariantPrimitiveType type, const IColumn & column, size_t row, UInt32 scale, Sink & sink)
{
    /// Writes a decimal primitive header, the scale (taken from the TYPE), then the raw value.
    const auto & decimal_column = assert_cast<const ColumnDecimal<T> &>(column);
    sink.writePrimitiveHeader(type);
    sink.writePOD(static_cast<UInt8>(scale));
    sink.writePOD(decimal_column.getData()[row]);
}

/// Encodes one residual scalar value read directly from `(column, row)`: first the type-hint stage
/// (`tryEncodeVariantScalarFromColumnUsingTypeHint`), then a fallback dispatch on the column's concrete
/// value type, mapping each through `NearestFieldType` to the matching `VARIANT` primitive.
template <typename Sink>
void encodeVariantScalarFromColumn(
    const IColumn & column, size_t row, const IDataType & value_type, bool from_dynamic, const DataTypePtr & type_hint, Sink & sink)
{
    /// `Bool` is `UInt8` under the hood. A value resolved from a `ColumnDynamic` whose declared type is
    /// `Bool` must encode as a boolean primitive, whereas a plain typed-path `UInt8`/`Bool` column falls
    /// through to the generic dispatch below and encodes as `Int64`. So the boolean encoding only applies
    /// to dynamic-resolved values.
    if (from_dynamic && value_type.getName() == "Bool")
    {
        bool value = assert_cast<const ColumnVector<UInt8> &>(column).getData()[row] != 0;
        sink.writePrimitiveHeader(value ? VariantPrimitiveType::BooleanTrue : VariantPrimitiveType::BooleanFalse);
        return;
    }

    if (tryEncodeVariantScalarFromColumnUsingTypeHint(column, row, value_type, type_hint, sink))
        return;

    switch (value_type.getTypeId())
    {
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Date32:
        case TypeIndex::Time:
        {
            /// All of these produce an `Int64` `Field`; encode as `Int64`.
            Int64 value = 0;
            tryReadColumnIntegralValue(column, row, value_type, value);
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, value, sink);
            return;
        }
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::Date:
        case TypeIndex::DateTime:
        {
            /// All of these produce a `UInt64` `Field`; encode as `Int64` with the same overflow check.
            UInt64 source = 0;
            tryReadColumnIntegralValue(column, row, value_type, source);
            if (source > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode integer {} as `Parquet` `VARIANT` `INT64`", source);

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, static_cast<Int64>(source), sink);
            return;
        }
        case TypeIndex::Int128:
        case TypeIndex::UInt128:
        case TypeIndex::Int256:
        case TypeIndex::UInt256:
        {
            Int64 converted = 0;
            if (!tryReadColumnIntegralValue(column, row, value_type, converted))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode integer value as `Parquet` `VARIANT` `INT64`");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, converted, sink);
            return;
        }
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        {
            /// A `Float32` column yields a `Float64` `Field` (`NearestFieldType<Float32> == Float64`),
            /// so both widen to `Float64` and encode as `Double`.
            Float64 source = value_type.getTypeId() == TypeIndex::Float32
                ? static_cast<Float64>(assert_cast<const ColumnVector<Float32> &>(column).getData()[row])
                : assert_cast<const ColumnVector<Float64> &>(column).getData()[row];
            if (!std::isfinite(source))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode non-finite `Parquet` `VARIANT` `DOUBLE`");

            sink.writePrimitiveHeader(VariantPrimitiveType::Double);
            sink.writePOD(source);
            return;
        }
        case TypeIndex::String:
        {
            sink.writeString(assert_cast<const ColumnString &>(column).getDataAt(row));
            return;
        }
        case TypeIndex::FixedString:
        {
            sink.writeString(assert_cast<const ColumnFixedString &>(column).getDataAt(row));
            return;
        }
        case TypeIndex::UUID:
        {
            sink.writePrimitiveHeader(VariantPrimitiveType::UUID);
            sink.writeUUID(assert_cast<const ColumnVector<UUID> &>(column).getData()[row]);
            return;
        }
        case TypeIndex::Decimal32:
            writeVariantDecimalFromColumn<Decimal32>(VariantPrimitiveType::Decimal4, column, row, getDecimalScale(value_type), sink);
            return;
        case TypeIndex::Decimal64:
            writeVariantDecimalFromColumn<Decimal64>(VariantPrimitiveType::Decimal8, column, row, getDecimalScale(value_type), sink);
            return;
        case TypeIndex::DateTime64:
            /// A `DateTime64` value is a `Decimal64` under the hood, so with no applicable type hint it is
            /// encoded as a `Decimal8` with the type's scale.
            writeVariantDecimalFromColumn<DateTime64>(VariantPrimitiveType::Decimal8, column, row, getDecimalScale(value_type), sink);
            return;
        case TypeIndex::Time64:
            writeVariantDecimalFromColumn<Time64>(VariantPrimitiveType::Decimal8, column, row, getDecimalScale(value_type), sink);
            return;
        case TypeIndex::Decimal128:
            writeVariantDecimalFromColumn<Decimal128>(VariantPrimitiveType::Decimal16, column, row, getDecimalScale(value_type), sink);
            return;
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
        {
            WriteBufferFromOwnString wb;
            if (value_type.getTypeId() == TypeIndex::IPv4)
                writeText(assert_cast<const ColumnVector<IPv4> &>(column).getData()[row], wb);
            else
                writeText(assert_cast<const ColumnVector<IPv6> &>(column).getData()[row], wb);
            String text = wb.str();
            sink.writeString(text);
            return;
        }
        case TypeIndex::Decimal256:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot encode `Decimal256` as `Parquet` `VARIANT`");
        default:
            break;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported value while encoding `Parquet` `VARIANT`");
}

/// Returns whether the unwrapped column value type is a residual scalar that `encodeVariantScalarFromColumn`
/// can encode. When it returns false the columnar cursor reports the value as unencodable instead of
/// emitting a scalar.
bool isSupportedDirectVariantResidualScalarColumnType(const IDataType & value_type)
{
    switch (value_type.getTypeId())
    {
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::Time:
        case TypeIndex::Time64:
        case TypeIndex::String:
        case TypeIndex::FixedString:
        case TypeIndex::UUID:
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
            return true;
        default:
            return false;
    }
}

size_t getVariantScalarEncodedSizeFromColumn(
    const IColumn & column, size_t row, const IDataType & value_type, bool from_dynamic, const DataTypePtr & type_hint)
{
    VariantScalarMeasureSink sink;
    encodeVariantScalarFromColumn(column, row, value_type, from_dynamic, type_hint, sink);
    return sink.size;
}

void writeVariantScalarFromColumnToBuffer(
    const IColumn & column, size_t row, const IDataType & value_type, bool from_dynamic, const DataTypePtr & type_hint, char *& out)
{
    VariantScalarWriteSink sink(out);
    encodeVariantScalarFromColumn(column, row, value_type, from_dynamic, type_hint, sink);
}

/// The types below implement the columnar `value`-payload cursor (`measureVariantColumnarValue` /
/// `writeVariantColumnarValue`). The cursor reads the binary `value` directly from `(column, type, row)`
/// and its sub-columns, without materializing a per-row `Field`/`Object`/`Array` tree. It encodes every
/// shape: scalars, nested `Object`/`Tuple`/`Map`/`Array`/`Variant`/`Dynamic`, and the genuinely per-row
/// heterogeneous cases (shared-`Variant` binary blobs, `ColumnConst`/`ColumnSparse`) which it
/// resolves without building a full row value tree. Object children (named `Tuple`,
/// `Map(String, T)`, reconstructed `ColumnObject` keys) are emitted in lexicographic key order.
///
/// A row-group-local arena that owns transient columns the cursor materializes for the genuinely
/// per-row heterogeneous cases (shared-`Variant` binary blobs and cached full sparse columns).
/// The cursor measures and writes a value in two passes that both re-resolve the source, so a value
/// canonicalized into a transient column must survive between them; this arena keeps every such column
/// alive for the whole encode and caches the decode by `(column pointer, row)` so it is paid once.
struct VariantSharedBlobCacheKey
{
    const ColumnString * column = nullptr;
    size_t row = 0;

    bool operator==(const VariantSharedBlobCacheKey & other) const
    {
        return column == other.column && row == other.row;
    }
};

struct VariantSharedBlobCacheKeyHash
{
    size_t operator()(const VariantSharedBlobCacheKey & key) const
    {
        size_t result = std::hash<const void *>{}(key.column);
        result ^= std::hash<size_t>{}(key.row) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
        return result;
    }
};

struct VariantColumnarTransientArena
{
    std::deque<ColumnPtr> owned_columns;
    std::unordered_map<VariantSharedBlobCacheKey, std::pair<const IColumn *, DataTypePtr>, VariantSharedBlobCacheKeyHash> shared_variant_cache;
    std::unordered_map<const IColumn *, ColumnPtr> full_sparse_columns;
};

struct VariantColumnarCursorContext
{
    const std::unordered_map<String, UInt32> & dictionary;
    const FormatSettings & format_settings;
    VariantColumnarTransientArena * arena = nullptr;
};

DataTypePtr getVariantColumnarScalarTypeHint(const DataTypePtr & type_hint, const DataTypePtr & resolved_type)
{
    DataTypePtr normalized_hint = unwrapVariantTypeHint(type_hint);
    if (!normalized_hint
        || typeid_cast<const DataTypeDynamic *>(normalized_hint.get())
        || normalized_hint->getTypeId() == TypeIndex::Variant)
        return resolved_type;

    return type_hint;
}

[[noreturn]] void throwCannotEncodeVariantColumnarValue(const DataTypePtr & type)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot encode value of type {} as `Parquet` `VARIANT`",
        type ? type->getName() : String("unknown"));
}

/// One object child entry, sortable by key for lexicographic emission. Carries the recursion target
/// (`column`, `type`, `row`, `type_hint`, `from_dynamic`) so the write pass can re-emit the child
/// value without re-resolving the parent.
struct VariantColumnarObjectChild
{
    String key;
    UInt32 field_id = 0;
    size_t child_size = 0;
    const IColumn * column = nullptr;
    DataTypePtr type;
    size_t row = 0;
    DataTypePtr type_hint;
    bool from_dynamic = false;
};

bool measureVariantColumnarValue(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const DataTypePtr & type_hint,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    size_t & out_size);

void writeVariantColumnarValue(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const DataTypePtr & type_hint,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    char *& out);

bool tryEncodeVariantColumnarValue(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    String & out);

std::pair<const IColumn *, DataTypePtr> materializeVariantSharedBlob(
    const ColumnString & shared_values, size_t index, VariantColumnarTransientArena & arena);

/// One flat leaf of a `ColumnObject` row: the dot-separated path segments relative to the encoded
/// sub-object, plus the resolved `(column, type, row)` source to encode the leaf value from. Shared-data
/// leaves are materialized into the arena so they look like an ordinary typed leaf here.
struct VariantColumnarObjectLeaf
{
    std::vector<String> segments;
    const IColumn * column = nullptr;
    DataTypePtr type;
    size_t row = 0;
    /// Whether the leaf value originates from a source with no declared type (a dynamic path or a
    /// shared-data entry), exactly like a `Dynamic` value. This drives the scalar encoder's
    /// `Bool`->boolean-primitive vs `Bool`->`Int64` decision: an untyped `Bool` must encode as the
    /// boolean primitive, while a declared-`Bool` typed path encodes as `Int64`.
    bool from_dynamic = false;
};

/// Collects every flat leaf present in `object_column` at `row` (typed paths, dynamic paths, and
/// shared-data entries decoded into the arena), splitting each stored path into segments. The typed-path
/// value types come from `object_data_type`; dynamic and shared-data leaves resolve their concrete type
/// per row through the cursor (so their `type` is `Dynamic` / the decoded blob type).
void collectVariantColumnarObjectLeaves(
    const ColumnObject & object_column,
    const DataTypeObject & object_data_type,
    size_t row,
    const VariantColumnarCursorContext & context,
    std::vector<VariantColumnarObjectLeaf> & out)
{
    out.clear();
    static const DataTypePtr dynamic_type = std::make_shared<DataTypeDynamic>();
    const auto & declared_typed_paths = object_data_type.getTypedPaths();

    for (const auto & [path, column] : object_column.getTypedPaths())
    {
        auto it = declared_typed_paths.find(path);
        VariantColumnarObjectLeaf leaf;
        leaf.segments = splitVariantObjectPath(path);
        leaf.column = column.get();
        leaf.type = it != declared_typed_paths.end() ? it->second : dynamic_type;
        leaf.row = row;
        out.emplace_back(std::move(leaf));
    }

    for (const auto & [path, column] : object_column.getDynamicPathsPtrs())
    {
        if (column->isNullAt(row))
            continue;
        VariantColumnarObjectLeaf leaf;
        leaf.segments = splitVariantObjectPath(path);
        leaf.column = column;
        leaf.type = dynamic_type;
        leaf.row = row;
        leaf.from_dynamic = true;
        out.emplace_back(std::move(leaf));
    }

    const auto & shared_data_offsets = object_column.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
    size_t begin = row == 0 ? 0 : shared_data_offsets[row - 1];
    size_t end = shared_data_offsets[row];
    for (size_t i = begin; i != end; ++i)
    {
        chassert(context.arena);
        auto [blob_column, blob_type] = materializeVariantSharedBlob(*shared_data_values, i, *context.arena);
        VariantColumnarObjectLeaf leaf;
        leaf.segments = splitVariantObjectPath(shared_data_paths->getDataAt(i));
        leaf.column = blob_column;
        leaf.type = blob_type;
        leaf.row = 0;
        /// Shared-data blobs are materialized into a concrete typed column, so the cursor's
        /// `resolveVariantColumnarSource` no longer sees a `Dynamic` to set `from_dynamic`. The value
        /// has no declared type (it lived in the shared-data store), so mark it dynamic here, otherwise
        /// a shared-data `Bool` would be mis-encoded as `Int64`.
        leaf.from_dynamic = true;
        out.emplace_back(std::move(leaf));
    }
}

/// A node of the nested object tree reconstructed from a `ColumnObject` row's flat leaves: either a
/// scalar/array/object leaf source `(column, type, row)` to recurse the cursor into, or a sub-object
/// with its own children. Children carry their encoded size for the two-pass measure/write.
struct VariantColumnarObjectColumnChild
{
    String key;
    UInt32 field_id = 0;
    size_t child_size = 0;
    const VariantColumnarObjectLeaf * leaf = nullptr;
    bool is_object = false;
    std::vector<VariantColumnarObjectColumnChild> children;
    UInt8 field_id_size = 0;
    UInt8 field_offset_size = 0;
    bool is_large = false;
};

/// Measures the encoded size of one reconstructed object level over `leaves[begin, end)` (already sorted
/// by segments) at `depth`, filling `out_children`/header widths and returning the total object size.
/// `excluded_top_keys`, when set and at `depth == 0`, skips those top-level keys (used to carve the
/// shredded fields out of the residual object).
size_t measureVariantColumnarObjectColumnLevel(
    const std::vector<VariantColumnarObjectLeaf> & leaves,
    size_t begin,
    size_t end,
    size_t depth,
    size_t object_depth,
    const VariantColumnarCursorContext & context,
    std::vector<VariantColumnarObjectColumnChild> & out_children,
    UInt8 & out_field_id_size,
    UInt8 & out_field_offset_size,
    bool & out_is_large,
    const std::unordered_set<String> * excluded_top_keys = nullptr)
{
    out_children.clear();
    UInt32 highest_field_id = 0;
    size_t total_children_size = 0;

    for (size_t pos = begin; pos < end;)
    {
        checkVariantWriteDepth(context.format_settings, object_depth + depth + 1);

        /// A residual leaf whose path ends above this level while a sibling descends through the same
        /// key means a key is used both as a scalar and as a sub-object (e.g. `{"b":1,"b":{"c":2}}`),
        /// which cannot be represented. Reject it rather than reading `segments[depth]` out of bounds.
        if (leaves[pos].segments.size() <= depth)
        {
            String conflicting_path;
            for (const String & segment : leaves[pos].segments)
            {
                if (!conflicting_path.empty())
                    conflicting_path += '.';
                conflicting_path += segment;
            }
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot prepare `Object` path {} for `Parquet` `VARIANT` writing because it conflicts with another residual value",
                conflicting_path);
        }

        const String & key = leaves[pos].segments[depth];
        size_t next = pos + 1;
        while (next < end && leaves[next].segments.size() > depth && leaves[next].segments[depth] == key)
            ++next;

        if (depth == 0 && excluded_top_keys && excluded_top_keys->contains(key))
        {
            pos = next;
            continue;
        }

        auto dict_it = context.dictionary.find(key);
        if (dict_it == context.dictionary.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing `Parquet` `VARIANT` dictionary entry for key {}", key);

        VariantColumnarObjectColumnChild child;
        child.key = key;
        child.field_id = dict_it->second;
        highest_field_id = std::max(highest_field_id, child.field_id);

        if (leaves[pos].segments.size() == depth + 1 && next == pos + 1)
        {
            child.leaf = &leaves[pos];
            if (!measureVariantColumnarValue(
                    *child.leaf->column,
                    child.leaf->type,
                    child.leaf->row,
                    child.leaf->type,
                    child.leaf->from_dynamic,
                    context,
                    object_depth + depth + 1,
                    child.child_size))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to measure `Parquet` `VARIANT` object leaf");
        }
        else
        {
            /// A key that is both a leaf and a sub-object prefix cannot exist in a valid `ColumnObject`
            /// row; treat the whole span as a nested object.
            child.is_object = true;
            child.child_size = measureVariantColumnarObjectColumnLevel(
                leaves, pos, next, depth + 1, object_depth, context, child.children,
                child.field_id_size, child.field_offset_size, child.is_large);
        }

        addVariantEncodingSizeOrThrow(total_children_size, child.child_size, "`object` children");
        out_children.emplace_back(std::move(child));
        pos = next;
    }

    size_t total_size = 0;
    finishVariantObjectEncodingHeader(
        out_children.size(), highest_field_id, total_children_size,
        out_field_id_size, out_field_offset_size, out_is_large, total_size);
    return total_size;
}

/// Writes one reconstructed object level previously measured by `measureVariantColumnarObjectColumnLevel`.
void writeVariantColumnarObjectColumnLevel(
    const std::vector<VariantColumnarObjectColumnChild> & children,
    UInt8 field_id_size,
    UInt8 field_offset_size,
    bool is_large,
    const VariantColumnarCursorContext & context,
    size_t depth,
    char *& out)
{
    checkVariantWriteDepth(context.format_settings, depth);
    writeVariantObjectHeaderAndChildren(
        children, field_id_size, field_offset_size, is_large, out,
        [&](const VariantColumnarObjectColumnChild & child)
        {
            if (child.is_object)
                writeVariantColumnarObjectColumnLevel(child.children, child.field_id_size, child.field_offset_size, child.is_large, context, depth + 1, out);
            else
                writeVariantColumnarValue(*child.leaf->column, child.leaf->type, child.leaf->row, child.leaf->type, child.leaf->from_dynamic, context, depth + 1, out);
        });
}

/// Sorts the collected leaves of a `ColumnObject` row by segments so equal first-segments are contiguous
/// at every depth, matching the lexicographic object emission order.
void sortVariantColumnarObjectLeaves(std::vector<VariantColumnarObjectLeaf> & leaves)
{
    std::sort(leaves.begin(), leaves.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs.segments < rhs.segments;
    });
}

/// Computes the encoded size of an array (header plus offset table) given its children sizes.
bool measureVariantColumnarArraySize(const std::vector<size_t> & child_sizes, size_t & out_size)
{
    size_t total_children_size = 0;
    for (size_t child_size : child_sizes)
        addVariantEncodingSizeOrThrow(total_children_size, child_size, "`array` children");

    UInt8 field_offset_size = variantByteLength(total_children_size);
    if (field_offset_size > 4)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Cannot encode `Parquet` `VARIANT` array header requiring offset size {}; maximum supported header width is 4 bytes",
            static_cast<UInt32>(field_offset_size));
    }

    if (child_sizes.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Cannot encode `Parquet` `VARIANT` array with more than {} elements", std::numeric_limits<UInt32>::max());

    bool is_large = child_sizes.size() > std::numeric_limits<UInt8>::max();
    size_t total_size = 1;
    addVariantEncodingSizeOrThrow(total_size, is_large ? sizeof(UInt32) : sizeof(UInt8), "`array` header");
    addVariantEncodingSizeOrThrow(
        total_size,
        multiplyVariantEncodingSizeOrThrow(
            addVariantEncodingSizesOrThrow(child_sizes.size(), size_t{1}, "`array` offset table size"),
            field_offset_size,
            "`array` field offset table"),
        "`array` total size");
    addVariantEncodingSizeOrThrow(total_size, total_children_size, "`array` total size");
    out_size = total_size;
    return true;
}

/// Writes the array header and offset table (not the children).
void writeVariantColumnarArrayHeader(const std::vector<size_t> & child_sizes, char *& out)
{
    size_t total_children_size = 0;
    for (size_t child_size : child_sizes)
        total_children_size += child_size;

    UInt8 field_offset_size = variantByteLength(total_children_size);
    bool is_large = child_sizes.size() > std::numeric_limits<UInt8>::max();

    UInt8 header = static_cast<UInt8>(VariantBasicType::Array);
    header |= static_cast<UInt8>(field_offset_size - 1) << VARIANT_VALUE_HEADER_SHIFT;
    header |= static_cast<UInt8>(is_large) << (VARIANT_VALUE_HEADER_SHIFT + VARIANT_ARRAY_IS_LARGE_SHIFT);
    *out = static_cast<char>(header);
    ++out;

    if (is_large)
        writeVariantPODToBuffer(static_cast<UInt32>(child_sizes.size()), out);
    else
    {
        *out = static_cast<char>(static_cast<UInt8>(child_sizes.size()));
        ++out;
    }

    UInt64 offset = 0;
    for (size_t child_size : child_sizes)
    {
        writeVariantLittleEndianToBuffer(offset, field_offset_size, out);
        addVariantEncodingUInt64OrThrow(offset, static_cast<UInt64>(child_size), "`array` child offsets");
    }
    writeVariantLittleEndianToBuffer(offset, field_offset_size, out);
}

/// Decodes a shared-`Variant` binary blob (the per-row payload `ColumnDynamic` stores in its shared
/// `ColumnString`) into a single-row typed column owned by the arena, caching the decode by
/// `(values column, index)` so the measure and write passes share it. Returns the materialized
/// `(column, type)` to recurse into.
std::pair<const IColumn *, DataTypePtr> materializeVariantSharedBlob(
    const ColumnString & shared_values, size_t index, VariantColumnarTransientArena & arena)
{
    VariantSharedBlobCacheKey key{.column = &shared_values, .row = index};
    auto it = arena.shared_variant_cache.find(key);
    if (it != arena.shared_variant_cache.end())
        return it->second;

    ReadBufferFromMemory buf(shared_values.getDataAt(index));
    auto nested_type = decodeDataType(buf);
    auto nested_column = nested_type->createColumn();
    nested_type->getDefaultSerialization()->deserializeBinary(*nested_column, buf, FormatSettings());

    arena.owned_columns.emplace_back(std::move(nested_column));
    std::pair<const IColumn *, DataTypePtr> resolved{arena.owned_columns.back().get(), std::move(nested_type)};
    arena.shared_variant_cache.emplace(key, resolved);
    return resolved;
}

/// Resolves a `(column, type, row)` to the underlying source the cursor should recurse into:
/// peels `Nullable`/`LowCardinality`/`Const`/`Sparse`, and for `Dynamic` resolves the per-row concrete
/// variant sub-column. A shared-`Variant` binary blob is canonicalized into a transient typed column in
/// the context arena and resolved from there, so no per-row `Field` tree is needed.
/// On a per-row `Null` (Nullable null, or `Dynamic` with no type), sets `out_is_null = true`.
bool resolveVariantColumnarSource(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const VariantColumnarCursorContext & context,
    const IColumn *& out_column,
    DataTypePtr & out_type,
    size_t & out_row,
    bool & out_from_dynamic,
    bool & out_is_null)
{
    const IColumn * current_column = &column;
    DataTypePtr current_type = unwrapVariantTypeHint(type);
    size_t current_row = row;
    bool from_dynamic = out_from_dynamic;

    if (const auto * column_const = typeid_cast<const ColumnConst *>(current_column))
    {
        return resolveVariantColumnarSource(
            column_const->getDataColumn(), current_type, 0, context,
            out_column, out_type, out_row, out_from_dynamic, out_is_null);
    }

    if (current_column->isSparse())
    {
        chassert(context.arena);
        auto [it, inserted] = context.arena->full_sparse_columns.emplace(current_column, nullptr);
        if (inserted)
            it->second = current_column->convertToFullIfNeeded();

        return resolveVariantColumnarSource(
            *it->second, current_type, current_row, context,
            out_column, out_type, out_row, out_from_dynamic, out_is_null);
    }

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(current_column))
    {
        if (nullable->isNullAt(current_row))
        {
            out_is_null = true;
            return true;
        }
        current_column = &nullable->getNestedColumn();
    }

    if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(current_column))
    {
        current_row = low_cardinality->getIndexAt(current_row);
        current_column = low_cardinality->getDictionary().getNestedColumn().get();
        if (const auto * nested_nullable = typeid_cast<const ColumnNullable *>(current_column))
        {
            if (nested_nullable->isNullAt(current_row))
            {
                out_is_null = true;
                return true;
            }
            current_column = &nested_nullable->getNestedColumn();
        }
    }

    if (typeid_cast<const DataTypeDynamic *>(current_type.get()))
    {
        const auto * dynamic_column = typeid_cast<const ColumnDynamic *>(current_column);
        if (!dynamic_column)
            return false;

        auto nested_type = dynamic_column->getTypeAt(current_row);
        if (!nested_type)
        {
            out_is_null = true;
            return true;
        }

        const auto & variant_column = dynamic_column->getVariantColumn();
        auto global_discr = variant_column.globalDiscriminatorAt(current_row);
        if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            out_is_null = true;
            return true;
        }

        /// Shared-variant values are binary blobs in a `ColumnString`; canonicalize the one value into
        /// a transient typed column in the arena and resolve from there.
        if (global_discr == dynamic_column->getSharedVariantDiscriminator())
        {
            chassert(context.arena);
            auto [blob_column, blob_type] = materializeVariantSharedBlob(
                dynamic_column->getSharedVariant(), variant_column.offsetAt(current_row), *context.arena);
            out_from_dynamic = true;
            return resolveVariantColumnarSource(
                *blob_column, blob_type, 0, context,
                out_column, out_type, out_row, out_from_dynamic, out_is_null);
        }

        out_from_dynamic = true;
        return resolveVariantColumnarSource(
            variant_column.getVariantByGlobalDiscriminator(global_discr),
            nested_type,
            variant_column.offsetAt(current_row),
            context,
            out_column,
            out_type,
            out_row,
            out_from_dynamic,
            out_is_null);
    }

    if (current_type->getTypeId() == TypeIndex::Variant)
    {
        const auto * variant_column = typeid_cast<const ColumnVariant *>(current_column);
        if (!variant_column)
            return false;

        auto global_discr = variant_column->globalDiscriminatorAt(current_row);
        if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            out_is_null = true;
            return true;
        }

        const auto & variant_type = assert_cast<const DataTypeVariant &>(*current_type);
        return resolveVariantColumnarSource(
            variant_column->getVariantByGlobalDiscriminator(global_discr),
            variant_type.getVariant(global_discr),
            variant_column->offsetAt(current_row),
            context,
            out_column,
            out_type,
            out_row,
            out_from_dynamic,
            out_is_null);
    }

    out_column = current_column;
    out_type = current_type;
    out_row = current_row;
    out_from_dynamic = from_dynamic;
    return true;
}

/// Collects an object child set (named `Tuple` or `Map`) into `children` sorted lexicographically by
/// key, recording each child's recursion target and encoded size, and computes the object's total
/// encoded size and header widths. Returns false on decline (any child the cursor cannot handle).
template <typename EmitChildren>
bool collectVariantColumnarObjectChildren(
    std::vector<VariantColumnarObjectChild> & children,
    size_t & out_total_size,
    UInt8 & out_field_id_size,
    UInt8 & out_field_offset_size,
    bool & out_is_large,
    const VariantColumnarCursorContext & context,
    size_t depth,
    EmitChildren && emit_children)
{
    checkVariantWriteDepth(context.format_settings, depth);
    children.clear();

    /// `add_child` measures a single child value and records its key, field id, size and recursion target.
    auto add_child = [&](const String & key, const IColumn & child_column, const DataTypePtr & child_type, size_t child_row, const DataTypePtr & child_type_hint, bool child_from_dynamic) -> bool
    {
        auto it = context.dictionary.find(key);
        if (it == context.dictionary.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing `Parquet` `VARIANT` dictionary entry for key {}", key);

        size_t child_size = 0;
        if (!measureVariantColumnarValue(child_column, child_type, child_row, child_type_hint, child_from_dynamic, context, depth + 1, child_size))
            return false;

        children.emplace_back(VariantColumnarObjectChild
        {
            .key = key,
            .field_id = it->second,
            .child_size = child_size,
            .column = &child_column,
            .type = child_type,
            .row = child_row,
            .type_hint = child_type_hint,
            .from_dynamic = child_from_dynamic,
        });
        return true;
    };

    if (!emit_children(add_child))
        return false;

    std::sort(children.begin(), children.end(), [](const auto & lhs, const auto & rhs) { return lhs.key < rhs.key; });

    UInt32 highest_field_id = 0;
    size_t total_children_size = 0;
    for (const auto & child : children)
    {
        highest_field_id = std::max(highest_field_id, child.field_id);
        addVariantEncodingSizeOrThrow(total_children_size, child.child_size, "`object` children");
    }

    finishVariantObjectEncodingHeader(
        children.size(), highest_field_id, total_children_size,
        out_field_id_size, out_field_offset_size, out_is_large, out_total_size);
    return true;
}

/// Writes an object value (header, field-id table, offset table, children) for the write pass,
/// collecting the children with `emit_children` (must not decline here — already validated by the
/// measure pass) and recursing each sorted child via `writeVariantColumnarValue`.
template <typename EmitChildren>
void writeVariantColumnarObject(char *& out, const VariantColumnarCursorContext & context, size_t depth, EmitChildren && emit_children)
{
    checkVariantWriteDepth(context.format_settings, depth);
    std::vector<VariantColumnarObjectChild> children;
    size_t total_size = 0;
    UInt8 field_id_size = 0;
    UInt8 field_offset_size = 0;
    bool is_large = false;
    bool ok = collectVariantColumnarObjectChildren(
        children, total_size, field_id_size, field_offset_size, is_large, context, depth, emit_children);
    if (!ok)
        throwCannotEncodeVariantColumnarValue({});

    writeVariantObjectHeaderAndChildren(
        children,
        field_id_size,
        field_offset_size,
        is_large,
        out,
        [&](const VariantColumnarObjectChild & child)
        {
            writeVariantColumnarValue(*child.column, child.type, child.row, child.type_hint, child.from_dynamic, context, depth + 1, out);
        });
}

/// Iterates the children of a named `Tuple` source column, invoking `add_child` per element.
template <typename AddChild>
bool emitVariantColumnarNamedTupleChildren(
    const ColumnTuple & tuple_column,
    const DataTypeTuple & tuple_type,
    size_t row,
    bool from_dynamic,
    AddChild && add_child)
{
    for (size_t i = 0; i < tuple_type.getElements().size(); ++i)
    {
        const String & key = tuple_type.getNameByPosition(i + 1);
        if (!add_child(key, tuple_column.getColumn(i), tuple_type.getElement(i), row, tuple_type.getElement(i), from_dynamic))
            return false;
    }
    return true;
}

/// Iterates the entries of a `Map(String, T)` source column for one row, invoking `add_child` per entry.
/// Throws for a non-`String`-keyed map and for a duplicate key within the row.
template <typename AddChild>
bool emitVariantColumnarMapChildren(
    const ColumnMap & map_column,
    const DataTypeMap & map_type,
    size_t row,
    bool from_dynamic,
    AddChild && add_child)
{
    const auto & offsets = map_column.getNestedColumn().getOffsets();
    size_t begin = row == 0 ? 0 : offsets[row - 1];
    size_t end = offsets[row];
    const auto & nested_data = map_column.getNestedData();
    const auto & keys_column = nested_data.getColumn(0);
    const auto & values_column = nested_data.getColumn(1);
    const DataTypePtr & value_type = map_type.getValueType();

    const auto * keys_string = typeid_cast<const ColumnString *>(&keys_column);
    if (!keys_string)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `Map(String, T)` can be written as `Parquet` `VARIANT`");

    std::unordered_set<std::string_view> seen_keys;
    seen_keys.reserve(end - begin);
    for (size_t i = begin; i != end; ++i)
    {
        std::string_view key_view = keys_string->getDataAt(i);
        if (!seen_keys.emplace(key_view).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate key {} while preparing `Parquet` `VARIANT` object", String(key_view));

        String key(key_view);
        if (!add_child(key, values_column, value_type, i, value_type, from_dynamic))
            return false;
    }
    return true;
}

bool measureVariantColumnarValue(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const DataTypePtr & type_hint,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    size_t & out_size)
{
    checkVariantWriteDepth(context.format_settings, depth);

    const IColumn * resolved_column = nullptr;
    DataTypePtr resolved_type;
    size_t resolved_row = 0;
    bool resolved_from_dynamic = from_dynamic;
    bool is_null = false;
    if (!resolveVariantColumnarSource(
            column, type, row, context, resolved_column, resolved_type, resolved_row, resolved_from_dynamic, is_null))
        return false;

    if (is_null)
    {
        /// `Null` primitive: 1-byte header.
        out_size = 1;
        return true;
    }

    DataTypePtr normalized_type = unwrapVariantTypeHint(resolved_type);

    if (const auto * object_data_type = typeid_cast<const DataTypeObject *>(normalized_type.get()))
    {
        const auto * object_column = typeid_cast<const ColumnObject *>(resolved_column);
        if (!object_column)
            return false;

        std::vector<VariantColumnarObjectLeaf> leaves;
        collectVariantColumnarObjectLeaves(*object_column, *object_data_type, resolved_row, context, leaves);
        sortVariantColumnarObjectLeaves(leaves);
        std::vector<VariantColumnarObjectColumnChild> children;
        UInt8 fid = 0, fos = 0;
        bool large = false;
        out_size = measureVariantColumnarObjectColumnLevel(leaves, 0, leaves.size(), 0, depth, context, children, fid, fos, large);
        return true;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_type.get()))
    {
        const auto * tuple_column = typeid_cast<const ColumnTuple *>(resolved_column);
        if (!tuple_column)
            return false;

        if (tuple_type->hasExplicitNames())
        {
            std::vector<VariantColumnarObjectChild> children;
            UInt8 field_id_size = 0;
            UInt8 field_offset_size = 0;
            bool is_large = false;
            return collectVariantColumnarObjectChildren(
                children, out_size, field_id_size, field_offset_size, is_large, context, depth, [&](auto && add_child)
                {
                    return emitVariantColumnarNamedTupleChildren(*tuple_column, *tuple_type, resolved_row, resolved_from_dynamic, add_child);
                });
        }

        /// Unnamed tuple -> array of its elements.
        size_t element_count = tuple_type->getElements().size();
        std::vector<size_t> child_sizes(element_count);
        for (size_t i = 0; i < element_count; ++i)
        {
            if (!measureVariantColumnarValue(
                    tuple_column->getColumn(i), tuple_type->getElement(i), resolved_row,
                    tuple_type->getElement(i), resolved_from_dynamic, context, depth + 1, child_sizes[i]))
                return false;
        }
        return measureVariantColumnarArraySize(child_sizes, out_size);
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_type.get()))
    {
        const auto * array_column = typeid_cast<const ColumnArray *>(resolved_column);
        if (!array_column)
            return false;

        const auto & offsets = array_column->getOffsets();
        size_t begin = resolved_row == 0 ? 0 : offsets[resolved_row - 1];
        size_t end = offsets[resolved_row];
        std::vector<size_t> child_sizes(end - begin);
        for (size_t i = begin; i != end; ++i)
        {
            if (!measureVariantColumnarValue(
                    array_column->getData(), array_type->getNestedType(), i,
                    array_type->getNestedType(), resolved_from_dynamic, context, depth + 1, child_sizes[i - begin]))
                return false;
        }
        return measureVariantColumnarArraySize(child_sizes, out_size);
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(normalized_type.get()))
    {
        const auto * map_column = typeid_cast<const ColumnMap *>(resolved_column);
        if (!map_column)
            return false;

        std::vector<VariantColumnarObjectChild> children;
        UInt8 field_id_size = 0;
        UInt8 field_offset_size = 0;
        bool is_large = false;
        return collectVariantColumnarObjectChildren(
            children, out_size, field_id_size, field_offset_size, is_large, context, depth, [&](auto && add_child)
            {
                return emitVariantColumnarMapChildren(*map_column, *map_type, resolved_row, resolved_from_dynamic, add_child);
            });
    }

    if (!normalized_type || !isSupportedDirectVariantResidualScalarColumnType(*normalized_type))
        return false;

    DataTypePtr scalar_type_hint = getVariantColumnarScalarTypeHint(type_hint, resolved_type);
    out_size = getVariantScalarEncodedSizeFromColumn(
        *resolved_column, resolved_row, *normalized_type, resolved_from_dynamic, scalar_type_hint);
    return true;
}

void writeVariantColumnarValue(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const DataTypePtr & type_hint,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    char *& out)
{
    checkVariantWriteDepth(context.format_settings, depth);

    const IColumn * resolved_column = nullptr;
    DataTypePtr resolved_type;
    size_t resolved_row = 0;
    bool resolved_from_dynamic = from_dynamic;
    bool is_null = false;
    bool resolved = resolveVariantColumnarSource(
        column, type, row, context, resolved_column, resolved_type, resolved_row, resolved_from_dynamic, is_null);
    if (!resolved)
        throwCannotEncodeVariantColumnarValue(type);

    if (is_null)
    {
        VariantScalarWriteSink sink(out);
        sink.writePrimitiveHeader(VariantPrimitiveType::Null);
        return;
    }

    DataTypePtr normalized_type = unwrapVariantTypeHint(resolved_type);

    if (const auto * object_data_type = typeid_cast<const DataTypeObject *>(normalized_type.get()))
    {
        const auto * object_column = assert_cast<const ColumnObject *>(resolved_column);
        std::vector<VariantColumnarObjectLeaf> leaves;
        collectVariantColumnarObjectLeaves(*object_column, *object_data_type, resolved_row, context, leaves);
        sortVariantColumnarObjectLeaves(leaves);
        std::vector<VariantColumnarObjectColumnChild> children;
        UInt8 fid = 0, fos = 0;
        bool large = false;
        measureVariantColumnarObjectColumnLevel(leaves, 0, leaves.size(), 0, depth, context, children, fid, fos, large);
        writeVariantColumnarObjectColumnLevel(children, fid, fos, large, context, depth, out);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_type.get()))
    {
        const auto * tuple_column = assert_cast<const ColumnTuple *>(resolved_column);
        if (tuple_type->hasExplicitNames())
        {
            writeVariantColumnarObject(out, context, depth, [&](auto && add_child)
            {
                return emitVariantColumnarNamedTupleChildren(*tuple_column, *tuple_type, resolved_row, resolved_from_dynamic, add_child);
            });
            return;
        }

        size_t element_count = tuple_type->getElements().size();
        std::vector<size_t> child_sizes(element_count);
        for (size_t i = 0; i < element_count; ++i)
        {
            bool ok = measureVariantColumnarValue(
                tuple_column->getColumn(i), tuple_type->getElement(i), resolved_row,
                tuple_type->getElement(i), resolved_from_dynamic, context, depth + 1, child_sizes[i]);
            if (!ok)
                throwCannotEncodeVariantColumnarValue(tuple_type->getElement(i));
        }
        writeVariantColumnarArrayHeader(child_sizes, out);
        for (size_t i = 0; i < element_count; ++i)
            writeVariantColumnarValue(
                tuple_column->getColumn(i), tuple_type->getElement(i), resolved_row,
                tuple_type->getElement(i), resolved_from_dynamic, context, depth + 1, out);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_type.get()))
    {
        const auto * array_column = assert_cast<const ColumnArray *>(resolved_column);
        const auto & offsets = array_column->getOffsets();
        size_t begin = resolved_row == 0 ? 0 : offsets[resolved_row - 1];
        size_t end = offsets[resolved_row];
        std::vector<size_t> child_sizes(end - begin);
        for (size_t i = begin; i != end; ++i)
        {
            bool ok = measureVariantColumnarValue(
                array_column->getData(), array_type->getNestedType(), i,
                array_type->getNestedType(), resolved_from_dynamic, context, depth + 1, child_sizes[i - begin]);
            if (!ok)
                throwCannotEncodeVariantColumnarValue(array_type->getNestedType());
        }
        writeVariantColumnarArrayHeader(child_sizes, out);
        for (size_t i = begin; i != end; ++i)
            writeVariantColumnarValue(
                array_column->getData(), array_type->getNestedType(), i,
                array_type->getNestedType(), resolved_from_dynamic, context, depth + 1, out);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(normalized_type.get()))
    {
        const auto * map_column = assert_cast<const ColumnMap *>(resolved_column);
        writeVariantColumnarObject(out, context, depth, [&](auto && add_child)
        {
            return emitVariantColumnarMapChildren(*map_column, *map_type, resolved_row, resolved_from_dynamic, add_child);
        });
        return;
    }

    if (!normalized_type || !isSupportedDirectVariantResidualScalarColumnType(*normalized_type))
        throwCannotEncodeVariantColumnarValue(resolved_type);

    DataTypePtr scalar_type_hint = getVariantColumnarScalarTypeHint(type_hint, resolved_type);
    writeVariantScalarFromColumnToBuffer(
        *resolved_column, resolved_row, *normalized_type, resolved_from_dynamic, scalar_type_hint, out);
}


struct VariantShreddedColumnarResult
{
    std::optional<String> residual_value;
    std::optional<Field> typed_value;
};

void encodeVariantShreddedTypedValueColumnar(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    bool from_dynamic,
    const DataTypePtr & shredded_type,
    const VariantColumnarCursorContext & context,
    size_t depth,
    VariantShreddedColumnarResult & out);

/// Builds the `{value, typed_value}` wrapper `Field` for one shredded child by recursing the columnar
/// shredded encoder on `(column, type, row)` against the child `typed_value` type. `from_dynamic` marks
/// a source with no declared type (a dynamic-path or shared-data leaf), so an untyped `Bool` spilling to
/// the residual encodes as the boolean primitive rather than `Int64`.
Field buildVariantShreddedWrapperFieldColumnar(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    bool from_dynamic,
    const DataTypePtr & wrapper_type,
    const VariantColumnarCursorContext & context,
    size_t depth)
{
    const IDataType & wrapper_data_type = typeid_cast<const DataTypeNullable *>(wrapper_type.get())
        ? *assert_cast<const DataTypeNullable &>(*wrapper_type).getNestedType()
        : *wrapper_type;
    const auto & tuple_type = assert_cast<const DataTypeTuple &>(wrapper_data_type);
    auto typed_pos = tuple_type.tryGetPositionByName("typed_value").value();
    auto value_pos = tuple_type.tryGetPositionByName("value").value();
    DataTypePtr typed_nested = tuple_type.getElement(typed_pos);
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(typed_nested.get()))
        typed_nested = nullable->getNestedType();

    VariantShreddedColumnarResult transformed;
    encodeVariantShreddedTypedValueColumnar(column, type, row, from_dynamic, typed_nested, context, depth, transformed);

    Tuple result(tuple_type.getElements().size());
    result[value_pos] = transformed.residual_value ? Field(*transformed.residual_value) : Field();
    result[typed_pos]
        = transformed.typed_value.has_value() ? *transformed.typed_value : tuple_type.getElement(typed_pos)->getDefault();
    return result;
}

/// Builds a single shredded child `Field` from a contiguous span of `ColumnObject` leaves whose first
/// segment is the field name. When the span is one leaf ending at the field, the child is that leaf's
/// value; otherwise the field is a sub-object and the leaves (with the leading segment stripped) become
/// a transient nested `ColumnObject`-free encoding via the cursor. Here we recurse the shredded encoder
/// on the leaf source: a single-leaf field uses its `(column, type, row)`; a multi-leaf field is a plain
/// JSON sub-object that no shredded sub-tree covers, so it spills entirely to the wrapper residual.
Field buildVariantShreddedObjectChildFromLeaves(
    const std::vector<VariantColumnarObjectLeaf> & leaves,
    size_t begin,
    size_t end,
    const DataTypePtr & wrapper_type,
    const VariantColumnarCursorContext & context,
    size_t depth);

/// Encodes the residual `value` for one row with the cursor.
std::optional<String> encodeVariantResidualColumnar(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth)
{
    String encoded;
    bool ok = tryEncodeVariantColumnarValue(column, type, row, from_dynamic, context, depth, encoded);
    if (!ok)
        throwCannotEncodeVariantColumnarValue(type);
    return encoded;
}

/// Collects object-like leaves from any source the shredded object branch accepts: a `ColumnObject`
/// (multi-segment flat leaves), a named `Tuple`, or a `Map(String, T)` (single-segment leaves). Returns
/// false if the source is not object-like.
bool collectVariantShreddedSourceLeaves(
    const IColumn & resolved_column,
    const DataTypePtr & resolved_normalized,
    size_t resolved_row,
    const VariantColumnarCursorContext & context,
    std::vector<VariantColumnarObjectLeaf> & out)
{
    out.clear();
    if (const auto * object_data_type = typeid_cast<const DataTypeObject *>(resolved_normalized.get()))
    {
        collectVariantColumnarObjectLeaves(
            assert_cast<const ColumnObject &>(resolved_column), *object_data_type, resolved_row, context, out);
        return true;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(resolved_normalized.get()))
    {
        if (!tuple_type->hasExplicitNames())
            return false;
        const auto & tuple_column = assert_cast<const ColumnTuple &>(resolved_column);
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            VariantColumnarObjectLeaf leaf;
            leaf.segments = {tuple_type->getNameByPosition(i + 1)};
            leaf.column = &tuple_column.getColumn(i);
            leaf.type = tuple_type->getElement(i);
            leaf.row = resolved_row;
            out.emplace_back(std::move(leaf));
        }
        return true;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(resolved_normalized.get()))
    {
        const auto & map_column = assert_cast<const ColumnMap &>(resolved_column);
        const auto & offsets = map_column.getNestedColumn().getOffsets();
        size_t begin = resolved_row == 0 ? 0 : offsets[resolved_row - 1];
        size_t end = offsets[resolved_row];
        const auto & nested_data = map_column.getNestedData();
        const auto * keys_string = typeid_cast<const ColumnString *>(&nested_data.getColumn(0));
        if (!keys_string)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `Map(String, T)` can be written as `Parquet` `VARIANT`");
        const auto & values_column = nested_data.getColumnPtr(1);
        std::unordered_set<std::string_view> seen;
        for (size_t i = begin; i != end; ++i)
        {
            std::string_view key_view = keys_string->getDataAt(i);
            if (!seen.emplace(key_view).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate key {} while preparing `Parquet` `VARIANT` object", String(key_view));
            VariantColumnarObjectLeaf leaf;
            leaf.segments = {String(key_view)};
            leaf.column = values_column.get();
            leaf.type = map_type->getValueType();
            leaf.row = i;
            out.emplace_back(std::move(leaf));
        }
        return true;
    }

    return false;
}

/// Encodes an object residual `value` from the sorted leaves, excluding the shredded top-level keys.
/// Returns `std::nullopt` when nothing remains (the object is fully shredded or empty).
std::optional<String> encodeVariantShreddedResidualObjectFromLeaves(
    const std::vector<VariantColumnarObjectLeaf> & sorted_leaves,
    const std::unordered_set<String> & shredded_field_names,
    const VariantColumnarCursorContext & context,
    size_t depth)
{
    std::vector<VariantColumnarObjectColumnChild> children;
    UInt8 fid = 0, fos = 0;
    bool large = false;
    size_t total = measureVariantColumnarObjectColumnLevel(
        sorted_leaves, 0, sorted_leaves.size(), 0, depth, context, children, fid, fos, large, &shredded_field_names);
    if (children.empty())
        return std::nullopt;

    String out(total, '\0');
    char * out_pos = out.data();
    writeVariantColumnarObjectColumnLevel(children, fid, fos, large, context, depth, out_pos);
    chassert(out_pos == out.data() + out.size());
    return out;
}

std::optional<String> encodeVariantColumnarObjectResidualForRow(
    const ColumnObject & object_column,
    const DataTypeObject & object_data_type,
    size_t row,
    const std::vector<String> & prefix,
    const std::unordered_set<String> & excluded_fields,
    const std::unordered_map<String, UInt32> & dictionary,
    const FormatSettings & format_settings)
{
    VariantColumnarTransientArena arena;
    const VariantColumnarCursorContext context{.dictionary = dictionary, .format_settings = format_settings, .arena = &arena};

    std::vector<VariantColumnarObjectLeaf> leaves;
    collectVariantColumnarObjectLeaves(object_column, object_data_type, row, context, leaves);

    /// Keep only leaves under `prefix`, stripping the matched prefix segments so the residual is
    /// encoded relative to the sub-object. Empty residual segments (the path is exactly the prefix)
    /// cannot occur in a valid `ColumnObject` row.
    std::vector<VariantColumnarObjectLeaf> residual_leaves;
    residual_leaves.reserve(leaves.size());
    for (auto & leaf : leaves)
    {
        if (leaf.segments.size() <= prefix.size())
            continue;
        bool under_prefix = true;
        for (size_t i = 0; i < prefix.size(); ++i)
        {
            if (leaf.segments[i] != prefix[i])
            {
                under_prefix = false;
                break;
            }
        }
        if (!under_prefix)
            continue;

        /// Enforce the write-depth limit on the residual object's nesting (one level per path
        /// segment, plus one for the enclosing object).
        checkVariantWriteDepth(format_settings, leaf.segments.size() + 1);

        leaf.segments.erase(leaf.segments.begin(), leaf.segments.begin() + static_cast<ssize_t>(prefix.size()));
        residual_leaves.emplace_back(std::move(leaf));
    }

    sortVariantColumnarObjectLeaves(residual_leaves);
    return encodeVariantShreddedResidualObjectFromLeaves(residual_leaves, excluded_fields, context, prefix.size() + 1);
}

Field buildVariantShreddedObjectChildFromLeaves(
    const std::vector<VariantColumnarObjectLeaf> & leaves,
    size_t begin,
    size_t end,
    const DataTypePtr & wrapper_type,
    const VariantColumnarCursorContext & context,
    size_t depth)
{
    /// A single leaf whose path ends at this field: recurse the shredded encoder on its source.
    if (end == begin + 1 && leaves[begin].segments.size() == 1)
        return buildVariantShreddedWrapperFieldColumnar(
            *leaves[begin].column, leaves[begin].type, leaves[begin].row, leaves[begin].from_dynamic, wrapper_type, context, depth + 1);

    /// A sub-object field (multiple leaves, or a deeper single leaf). The shredded sub-type for this
    /// field must be an object `Tuple`; encode its shredded children from the stripped sub-leaves and the
    /// rest into the wrapper residual. Strip the leading segment from each leaf to get the sub-object.
    const IDataType & wrapper_data_type = typeid_cast<const DataTypeNullable *>(wrapper_type.get())
        ? *assert_cast<const DataTypeNullable &>(*wrapper_type).getNestedType()
        : *wrapper_type;
    const auto & wrapper_tuple = assert_cast<const DataTypeTuple &>(wrapper_data_type);
    auto typed_pos = wrapper_tuple.tryGetPositionByName("typed_value").value();
    auto value_pos = wrapper_tuple.tryGetPositionByName("value").value();
    DataTypePtr typed_nested = wrapper_tuple.getElement(typed_pos);
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(typed_nested.get()))
        typed_nested = nullable->getNestedType();

    std::vector<VariantColumnarObjectLeaf> sub_leaves;
    sub_leaves.reserve(end - begin);
    for (size_t i = begin; i != end; ++i)
    {
        VariantColumnarObjectLeaf leaf = leaves[i];
        leaf.segments.assign(leaves[i].segments.begin() + 1, leaves[i].segments.end());
        sub_leaves.emplace_back(std::move(leaf));
    }

    Tuple result(wrapper_tuple.getElements().size());

    if (const auto * sub_tuple_type = typeid_cast<const DataTypeTuple *>(unwrapVariantTypeHint(typed_nested).get()))
    {
        std::unordered_set<String> sub_shredded_names;
        Tuple typed_fields(sub_tuple_type->getElements().size());
        for (size_t i = 0; i < sub_tuple_type->getElements().size(); ++i)
        {
            const String & field_name = sub_tuple_type->getNameByPosition(i + 1);
            sub_shredded_names.emplace(field_name);
            size_t fbegin = sub_leaves.size();
            size_t fend = sub_leaves.size();
            for (size_t j = 0; j < sub_leaves.size(); ++j)
            {
                if (!sub_leaves[j].segments.empty() && sub_leaves[j].segments[0] == field_name)
                {
                    if (fbegin == sub_leaves.size())
                        fbegin = j;
                    fend = j + 1;
                }
            }
            if (fbegin == sub_leaves.size())
                typed_fields[i] = Field();
            else
                typed_fields[i] = buildVariantShreddedObjectChildFromLeaves(
                    sub_leaves, fbegin, fend, sub_tuple_type->getElement(i), context, depth + 1);
        }
        result[typed_pos] = Field(std::move(typed_fields));
        result[value_pos] = encodeVariantShreddedResidualObjectFromLeaves(sub_leaves, sub_shredded_names, context, depth + 1)
            .transform([](String s) { return Field(std::move(s)); })
            .value_or(Field());
        return result;
    }

    /// The shredded sub-type is not an object: the whole sub-object spills to the wrapper residual.
    result[typed_pos] = wrapper_tuple.getElement(typed_pos)->getDefault();
    result[value_pos] = encodeVariantShreddedResidualObjectFromLeaves(sub_leaves, {}, context, depth + 1)
        .transform([](String s) { return Field(std::move(s)); })
        .value_or(Field());
    return result;
}

void encodeVariantShreddedTypedValueColumnar(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    bool from_dynamic,
    const DataTypePtr & shredded_type,
    const VariantColumnarCursorContext & context,
    size_t depth,
    VariantShreddedColumnarResult & out)
{
    checkVariantWriteDepth(context.format_settings, depth);

    DataTypePtr normalized_shredded = unwrapVariantTypeHint(shredded_type);

    const IColumn * resolved_column = nullptr;
    DataTypePtr resolved_type;
    size_t resolved_row = 0;
    bool resolved_from_dynamic = from_dynamic;
    bool is_null = false;
    bool resolved = resolveVariantColumnarSource(
        column, type, row, context, resolved_column, resolved_type, resolved_row, resolved_from_dynamic, is_null);
    if (!resolved)
        throwCannotEncodeVariantColumnarValue(type);
    DataTypePtr resolved_normalized = is_null ? DataTypePtr{} : unwrapVariantTypeHint(resolved_type);

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_shredded.get()))
    {
        std::vector<VariantColumnarObjectLeaf> leaves;
        if (is_null
            || !collectVariantShreddedSourceLeaves(*resolved_column, resolved_normalized, resolved_row, context, leaves))
        {
            out.residual_value = encodeVariantResidualColumnar(column, type, row, from_dynamic, context, depth);
            out.typed_value = std::nullopt;
            return;
        }

        sortVariantColumnarObjectLeaves(leaves);
        std::unordered_set<String> shredded_field_names;
        Tuple object_fields(tuple_type->getElements().size());
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            const String & field_name = tuple_type->getNameByPosition(i + 1);
            shredded_field_names.emplace(field_name);
            size_t begin = leaves.size();
            size_t end = leaves.size();
            for (size_t j = 0; j < leaves.size(); ++j)
            {
                if (!leaves[j].segments.empty() && leaves[j].segments[0] == field_name)
                {
                    if (begin == leaves.size())
                        begin = j;
                    end = j + 1;
                }
            }
            if (begin == leaves.size())
                object_fields[i] = Field();
            else
                object_fields[i] = buildVariantShreddedObjectChildFromLeaves(
                    leaves, begin, end, tuple_type->getElement(i), context, depth + 1);
        }

        out.residual_value = encodeVariantShreddedResidualObjectFromLeaves(leaves, shredded_field_names, context, depth);
        out.typed_value = Field(std::move(object_fields));
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_shredded.get()))
    {
        const auto * array_data_type = is_null ? nullptr : typeid_cast<const DataTypeArray *>(resolved_normalized.get());
        const auto * unnamed_tuple = is_null ? nullptr : typeid_cast<const DataTypeTuple *>(resolved_normalized.get());
        if (!array_data_type && !(unnamed_tuple && !unnamed_tuple->hasExplicitNames()))
        {
            out.residual_value = encodeVariantResidualColumnar(column, type, row, from_dynamic, context, depth);
            out.typed_value = std::nullopt;
            return;
        }

        Array values;
        if (array_data_type)
        {
            const auto * array_column = assert_cast<const ColumnArray *>(resolved_column);
            const auto & offsets = array_column->getOffsets();
            size_t begin = resolved_row == 0 ? 0 : offsets[resolved_row - 1];
            size_t end = offsets[resolved_row];
            values.reserve(end - begin);
            for (size_t i = begin; i != end; ++i)
                values.emplace_back(buildVariantShreddedWrapperFieldColumnar(
                    array_column->getData(), array_data_type->getNestedType(), i, resolved_from_dynamic, array_type->getNestedType(), context, depth + 1));
        }
        else
        {
            const auto * tuple_column = assert_cast<const ColumnTuple *>(resolved_column);
            values.reserve(unnamed_tuple->getElements().size());
            for (size_t i = 0; i < unnamed_tuple->getElements().size(); ++i)
                values.emplace_back(buildVariantShreddedWrapperFieldColumnar(
                    tuple_column->getColumn(i), unnamed_tuple->getElement(i), resolved_row, resolved_from_dynamic, array_type->getNestedType(), context, depth + 1));
        }

        out.residual_value = std::nullopt;
        out.typed_value = Field(std::move(values));
        return;
    }

    /// Scalar shredded type: convert the resolved leaf, else spill to residual.
    if (!is_null)
    {
        /// A `Bool` column yields a `UInt64` `Field` from `operator[]` (unless wrapped by a
        /// `ColumnDynamic`, whose `operator[]` runs `convertFieldToType`); reconstruct the `Bool`
        /// `Field` so it converts to a `Bool` shredded leaf instead of spilling to the residual.
        Field leaf = isBool(resolved_normalized)
            ? Field(assert_cast<const ColumnVector<UInt8> &>(*resolved_column).getData()[resolved_row] != 0)
            : (*resolved_column)[resolved_row];
        if (auto typed_scalar = tryConvertVariantScalarToShreddedField(leaf, normalized_shredded))
        {
            out.residual_value = std::nullopt;
            out.typed_value = std::move(typed_scalar);
            return;
        }
    }

    out.residual_value = encodeVariantResidualColumnar(column, type, row, from_dynamic, context, depth);
    out.typed_value = std::nullopt;
}

/// Encodes the full `value` payload for one row columnar. Unsupported reachable shapes throw;
/// false means the root value could not be resolved.
bool tryEncodeVariantColumnarValue(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    String & out)
{
    checkVariantWriteDepth(context.format_settings, depth);

    /// At the root, the type hint is the resolved concrete type of the top-level value. For a nested
    /// `Dynamic` element the hint stays the declared `Dynamic` element type instead, which is why the
    /// hint is threaded explicitly rather than re-derived from the concrete type.
    const IColumn * resolved_column = nullptr;
    DataTypePtr root_type_hint;
    size_t resolved_row = 0;
    bool resolved_from_dynamic = from_dynamic;
    bool is_null = false;
    if (!resolveVariantColumnarSource(column, type, row, context, resolved_column, root_type_hint, resolved_row, resolved_from_dynamic, is_null))
        return false;
    if (is_null)
        root_type_hint = type;

    size_t total_size = 0;
    if (!measureVariantColumnarValue(column, type, row, root_type_hint, from_dynamic, context, depth, total_size))
        throwCannotEncodeVariantColumnarValue(root_type_hint ? root_type_hint : type);

    out.resize(total_size);
    char * out_pos = out.data();
    writeVariantColumnarValue(column, type, row, root_type_hint, from_dynamic, context, depth, out_pos);
    chassert(out_pos == out.data() + out.size());
    return true;
}

VariantEncodingContext buildVariantEncodingContext(const std::unordered_set<String> & unique_keys)
{
    std::vector<String> keys;
    keys.reserve(unique_keys.size());
    for (const auto & key : unique_keys)
        keys.emplace_back(key);

    std::sort(keys.begin(), keys.end());

    UInt64 dictionary_bytes = 0;
    for (const auto & key : keys)
        addVariantEncodingUInt64OrThrow(dictionary_bytes, static_cast<UInt64>(key.size()), "`metadata` dictionary");

    if (keys.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "The `Parquet` `VARIANT` dictionary has more than {} entries", std::numeric_limits<UInt32>::max());

    if (dictionary_bytes > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "The total length of the `Parquet` `VARIANT` dictionary exceeds 4 GiB");

    VariantEncodingContext context;
    context.dictionary.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        context.dictionary.emplace(keys[i], static_cast<UInt32>(i));

    UInt8 offset_size = std::max(variantByteLength(static_cast<UInt64>(keys.size())), variantByteLength(dictionary_bytes));
    UInt8 header = 1;
    header |= static_cast<UInt8>(1) << 4;
    header |= static_cast<UInt8>(offset_size - 1) << 6;
    context.metadata.push_back(static_cast<char>(header));
    appendVariantLittleEndian(keys.size(), offset_size, context.metadata);

    UInt64 offset = 0;
    for (const auto & key : keys)
    {
        appendVariantLittleEndian(offset, offset_size, context.metadata);
        addVariantEncodingUInt64OrThrow(offset, static_cast<UInt64>(key.size()), "`metadata` dictionary offsets");
    }
    appendVariantLittleEndian(offset, offset_size, context.metadata);
    for (const auto & key : keys)
        context.metadata += key;

    return context;
}

/// Fast columnar encoder for the simplest shredded case: a top-level `Dynamic` column whose inferred or
/// declared shredded type is a single scalar. For each row the leaf scalar is read directly from the
/// resolved sub-column and converted to the shredded type via `tryConvertVariantScalarToShreddedField`.
/// The whole column declines (returns `std::nullopt`) the moment any row neither converts to the scalar
/// nor is a plain `Null` (the generic per-row columnar encoder then handles it). On the accepted shapes
/// there are never any object keys, so the shared `metadata` is the empty dictionary.
std::optional<PreparedVariantColumns> tryPrepareScalarShreddedVariantColumns(
    const IColumn & full_column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    const DataTypePtr & shredded_type)
{
    if (!shredded_type || !typeid_cast<const DataTypeDynamic *>(type.get()))
        return std::nullopt;

    DataTypePtr scalar_shredded = unwrapVariantTypeHint(shredded_type);
    if (!scalar_shredded
        || typeid_cast<const DataTypeTuple *>(scalar_shredded.get())
        || typeid_cast<const DataTypeArray *>(scalar_shredded.get())
        || typeid_cast<const DataTypeMap *>(scalar_shredded.get()))
        return std::nullopt;

    const size_t num_rows = full_column.size();

    static const std::unordered_map<String, UInt32> empty_dictionary;
    VariantColumnarTransientArena arena;
    const VariantColumnarCursorContext columnar_context{.dictionary = empty_dictionary, .format_settings = format_settings, .arena = &arena};

    auto typed_value_type = std::make_shared<DataTypeNullable>(shredded_type);
    auto typed_nested = typed_value_type->createColumn();
    auto & typed_nullable = assert_cast<ColumnNullable &>(*typed_nested);
    auto & typed_inner = typed_nullable.getNestedColumn();
    auto & typed_null_map = typed_nullable.getNullMapData();
    typed_inner.reserve(num_rows);
    typed_null_map.reserve(num_rows);

    /// The residual `value` is `Nullable(String)`. For an accepted row that converts to the scalar the
    /// residual is absent (null); for a plain `Null` row it is the 1-byte `Null` primitive.
    auto value_type = std::make_shared<DataTypeNullable>(getVariantStringType());
    auto value_column = value_type->createColumn();
    auto & value_nullable = assert_cast<ColumnNullable &>(*value_column);
    auto & value_inner = assert_cast<ColumnString &>(value_nullable.getNestedColumn());
    auto & value_null_map = value_nullable.getNullMapData();
    value_inner.reserve(num_rows);
    value_null_map.reserve(num_rows);

    char null_primitive = 0;
    {
        char * out = &null_primitive;
        VariantScalarWriteSink sink(out);
        sink.writePrimitiveHeader(VariantPrimitiveType::Null);
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        const IColumn * resolved_column = nullptr;
        DataTypePtr resolved_type;
        size_t resolved_row = 0;
        bool resolved_from_dynamic = false;
        bool is_null = false;
        if (!resolveVariantColumnarSource(full_column, type, row, columnar_context, resolved_column, resolved_type, resolved_row, resolved_from_dynamic, is_null))
            return std::nullopt;

        if (is_null)
        {
            /// `Null` -> residual is the `Null` primitive, `typed_value` is the default.
            value_inner.insertData(&null_primitive, 1);
            value_null_map.push_back(UInt8(0));
            typed_inner.insertDefault();
            typed_null_map.push_back(UInt8(1));
            continue;
        }

        /// A `Bool` column yields a `UInt64` `Field` from `operator[]`; reconstruct the `Bool` `Field`
        /// so it converts to a `Bool` shredded leaf instead of declining to the generic path.
        DataTypePtr resolved_normalized = unwrapVariantTypeHint(resolved_type);
        Field leaf = isBool(resolved_normalized)
            ? Field(assert_cast<const ColumnVector<UInt8> &>(*resolved_column).getData()[resolved_row] != 0)
            : (*resolved_column)[resolved_row];
        auto converted = tryConvertVariantScalarToShreddedField(leaf, scalar_shredded);
        if (!converted.has_value())
            return std::nullopt;

        typed_inner.insert(*converted);
        typed_null_map.push_back(UInt8(0));
        value_inner.insertDefault();
        value_null_map.push_back(UInt8(1));
    }

    VariantEncodingContext shared_context = buildVariantEncodingContext({});

    PreparedVariantColumns result;
    result.metadata_type = getVariantStringType();
    result.metadata_column = buildVariantMetadataColumn(num_rows, shared_context.metadata);
    result.value_type = std::move(value_type);
    result.value_column = std::move(value_column);
    result.typed_value_type = std::move(typed_value_type);
    result.typed_value_column = std::move(typed_nested);
    return result;
}




/// Columnar analysis walk: visits
/// `(column, type, row)` directly, recording shredding statistics into `node` and (when `keys` is set)
/// the union of object keys. The structure mirrors the residual cursor's value walk, except it descends
/// into nested `Object`/`Variant`/`Dynamic` and aggregates per-path scalar types instead of encoding.
void analyzeVariantColumnarValue(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    VariantWriteAnalysisNode * node,
    std::unordered_set<String> * keys);

/// Bumps the array statistics on `node` and recurses each element into the (single) array-child node.
void analyzeVariantColumnarArray(
    const IColumn & data_column,
    const DataTypePtr & element_type,
    size_t begin,
    size_t end,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    VariantWriteAnalysisNode * node,
    std::unordered_set<String> * keys)
{
    checkVariantWriteDepth(context.format_settings, depth);

    if (node)
    {
        ++node->value_count;
        ++node->array_count;
        if (!node->array_child)
            node->array_child = std::make_unique<VariantWriteAnalysisNode>();
    }
    VariantWriteAnalysisNode * child_node = node ? node->array_child.get() : nullptr;
    for (size_t i = begin; i != end; ++i)
        analyzeVariantColumnarValue(data_column, element_type, i, from_dynamic, context, depth + 1, child_node, keys);
}

/// Bumps the object statistics on `node` and recurses one named child into `object_fields[key]`.
void analyzeVariantColumnarObjectChild(
    const String & key,
    const IColumn & child_column,
    const DataTypePtr & child_type,
    size_t child_row,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    VariantWriteAnalysisNode * node,
    std::unordered_set<String> * keys)
{
    if (keys)
        keys->emplace(key);
    VariantWriteAnalysisNode * child_node = node ? &node->object_fields[key] : nullptr;
    analyzeVariantColumnarValue(child_column, child_type, child_row, from_dynamic, context, depth + 1, child_node, keys);
}

void analyzeVariantColumnarValue(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    bool from_dynamic,
    const VariantColumnarCursorContext & context,
    size_t depth,
    VariantWriteAnalysisNode * node,
    std::unordered_set<String> * keys)
{
    checkVariantWriteDepth(context.format_settings, depth);

    const IColumn * resolved_column = nullptr;
    DataTypePtr resolved_type;
    size_t resolved_row = 0;
    bool resolved_from_dynamic = from_dynamic;
    bool is_null = false;
    bool resolved = resolveVariantColumnarSource(
        column, type, row, context, resolved_column, resolved_type, resolved_row, resolved_from_dynamic, is_null);
    if (!resolved)
        throwCannotEncodeVariantColumnarValue(type);

    if (is_null)
    {
        if (node)
            ++node->value_count;
        return;
    }

    DataTypePtr normalized_type = unwrapVariantTypeHint(resolved_type);

    if (const auto * object_data_type = typeid_cast<const DataTypeObject *>(normalized_type.get()))
    {
        const auto & object_column = assert_cast<const ColumnObject &>(*resolved_column);
        std::vector<VariantColumnarObjectLeaf> leaves;
        collectVariantColumnarObjectLeaves(object_column, *object_data_type, resolved_row, context, leaves);
        sortVariantColumnarObjectLeaves(leaves);

        if (node)
        {
            ++node->value_count;
            ++node->object_count;
        }
        /// Walk the reconstructed object tree, recursing each top-level key span.
        std::function<void(const std::vector<VariantColumnarObjectLeaf> &, size_t, size_t, size_t, VariantWriteAnalysisNode *)> walk;
        walk = [&](const std::vector<VariantColumnarObjectLeaf> & lvs, size_t b, size_t e, size_t object_level, VariantWriteAnalysisNode * obj_node)
        {
            for (size_t pos = b; pos < e;)
            {
                checkVariantWriteDepth(context.format_settings, depth + object_level + 1);

                const String & key = lvs[pos].segments[object_level];
                size_t next = pos + 1;
                while (next < e && lvs[next].segments.size() > object_level && lvs[next].segments[object_level] == key)
                    ++next;

                if (keys)
                    keys->emplace(key);
                VariantWriteAnalysisNode * child_node = obj_node ? &obj_node->object_fields[key] : nullptr;
                if (lvs[pos].segments.size() == object_level + 1 && next == pos + 1)
                {
                    analyzeVariantColumnarValue(
                        *lvs[pos].column, lvs[pos].type, lvs[pos].row, false, context, depth + object_level + 1, child_node, keys);
                }
                else
                {
                    if (child_node)
                    {
                        ++child_node->value_count;
                        ++child_node->object_count;
                    }
                    walk(lvs, pos, next, object_level + 1, child_node);
                }
                pos = next;
            }
        };
        walk(leaves, 0, leaves.size(), 0, node);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_type.get()))
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*resolved_column);
        if (tuple_type->hasExplicitNames())
        {
            if (node)
            {
                ++node->value_count;
                ++node->object_count;
            }
            for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
                analyzeVariantColumnarObjectChild(
                    tuple_type->getNameByPosition(i + 1), tuple_column.getColumn(i), tuple_type->getElement(i),
                    resolved_row, resolved_from_dynamic, context, depth, node, keys);
            return;
        }

        /// Unnamed tuple is an array of its elements.
        if (node)
        {
            ++node->value_count;
            ++node->array_count;
            if (!node->array_child)
                node->array_child = std::make_unique<VariantWriteAnalysisNode>();
        }
        VariantWriteAnalysisNode * child_node = node ? node->array_child.get() : nullptr;
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
            analyzeVariantColumnarValue(
                tuple_column.getColumn(i), tuple_type->getElement(i), resolved_row,
                resolved_from_dynamic, context, depth + 1, child_node, keys);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_type.get()))
    {
        const auto & array_column = assert_cast<const ColumnArray &>(*resolved_column);
        const auto & offsets = array_column.getOffsets();
        size_t begin = resolved_row == 0 ? 0 : offsets[resolved_row - 1];
        size_t end = offsets[resolved_row];
        analyzeVariantColumnarArray(
            array_column.getData(), array_type->getNestedType(), begin, end,
            resolved_from_dynamic, context, depth, node, keys);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(normalized_type.get()))
    {
        const auto & map_column = assert_cast<const ColumnMap &>(*resolved_column);
        const auto & moffsets = map_column.getNestedColumn().getOffsets();
        size_t begin = resolved_row == 0 ? 0 : moffsets[resolved_row - 1];
        size_t end = moffsets[resolved_row];
        const auto & nested_data = map_column.getNestedData();
        const auto & keys_column = assert_cast<const ColumnString &>(nested_data.getColumn(0));
        const auto & values_column = nested_data.getColumn(1);
        if (node)
        {
            ++node->value_count;
            ++node->object_count;
        }
        for (size_t i = begin; i != end; ++i)
            analyzeVariantColumnarObjectChild(
                String(keys_column.getDataAt(i)), values_column, map_type->getValueType(), i,
                resolved_from_dynamic, context, depth, node, keys);
        return;
    }

    /// Scalar leaf. The analyze hint is the resolved concrete type: for a typed leaf it equals the
    /// declared `type_hint`, and for a `Dynamic`/`Variant`-resolved leaf it is the per-row concrete type
    /// (so e.g. a `Dynamic` `UInt8` is analyzed as `UInt8`, not widened to the `UInt64` `Field` type).
    if (node)
    {
        ++node->value_count;
        Field leaf = isBool(normalized_type)
            ? Field(assert_cast<const ColumnVector<UInt8> &>(*resolved_column).getData()[resolved_row] != 0)
            : (*resolved_column)[resolved_row];
        if (auto scalar_type = getVariantAnalyzeScalarType(leaf, normalized_type))
            addVariantAnalyzeScalarType(*node, scalar_type);
    }
}

}

PreparedVariantColumns prepareVariantColumnsForWrite(
    const ColumnPtr & column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    DataTypePtr shredded_type,
    DataTypePtr * out_shredded_type)
{
    ColumnPtr full_column_ptr = column->convertToFullColumnIfLowCardinality();
    const IColumn & full_column = *full_column_ptr;
    const auto * object_type = typeid_cast<const DataTypeObject *>(type.get());

    const size_t num_rows = full_column.size();
    const bool need_inference = !shredded_type && out_shredded_type;
    if (!need_inference)
    {
        if (auto prepared = tryPrepareObjectVariantColumnsFast(full_column, type, format_settings, shredded_type))
            return *prepared;
        if (auto prepared = tryPrepareScalarShreddedVariantColumns(full_column, type, format_settings, shredded_type))
            return *prepared;
    }

    /// Collect the object-key union (for the metadata dictionary) and, when inferring, the shredding
    /// statistics — both directly from the columns, with no per-row `Field` tree.
    std::unordered_set<String> unique_keys;
    VariantWriteAnalysisNode analysis;
    {
        VariantColumnarTransientArena analyze_arena;
        static const std::unordered_map<String, UInt32> empty_dictionary;
        const VariantColumnarCursorContext analyze_context{.dictionary = empty_dictionary, .format_settings = format_settings, .arena = &analyze_arena};
        for (size_t row = 0; row < num_rows; ++row)
            analyzeVariantColumnarValue(
                full_column, type, row, false, analyze_context, 1, need_inference ? &analysis : nullptr, &unique_keys);
    }

    if (need_inference)
    {
        if (object_type)
        {
            auto selected_paths = selectShreddedPathsForObject(analysis, *object_type);
            shredded_type = constructShreddedType(analysis, &selected_paths);
        }
        else
        {
            shredded_type = constructShreddedType(analysis);
        }
        *out_shredded_type = shredded_type;
    }

    /// The columnar preparers handle the common declared/inferred shredded shapes wholesale; if none
    /// applies, the generic per-row columnar encoder below covers every remaining shape.
    if (shredded_type)
    {
        if (object_type)
        {
            if (auto prepared = tryPrepareObjectVariantColumnsFast(full_column, type, format_settings, shredded_type))
                return *prepared;
        }
        else if (auto prepared = tryPrepareScalarShreddedVariantColumns(full_column, type, format_settings, shredded_type))
            return *prepared;
    }

    VariantEncodingContext shared_context = buildVariantEncodingContext(unique_keys);

    PreparedVariantColumns result;
    result.metadata_type = std::make_shared<DataTypeString>();

    MutableColumnPtr metadata = result.metadata_type->createColumn();
    MutableColumnPtr value;
    MutableColumnPtr typed_value;
    Field default_typed_value;

    if (shredded_type)
    {
        result.value_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        result.typed_value_type = std::make_shared<DataTypeNullable>(shredded_type);
        value = result.value_type->createColumn();
        typed_value = result.typed_value_type->createColumn();
        default_typed_value = result.typed_value_type->getDefault();
    }
    else
    {
        result.value_type = std::make_shared<DataTypeString>();
        value = result.value_type->createColumn();
    }

    metadata->reserve(num_rows);
    value->reserve(num_rows);
    if (typed_value)
        typed_value->reserve(num_rows);

    auto & metadata_string_column = assert_cast<ColumnString &>(*metadata);
    size_t metadata_row_size = addVariantEncodingSizesOrThrow(shared_context.metadata.size(), 1, "`metadata` row size");
    metadata_string_column.getChars().reserve(multiplyVariantEncodingSizeOrThrow(metadata_row_size, num_rows, "`metadata` column size"));

    /// The columnar cursor encodes the whole `value` payload (and, for the shredded case, the residual
    /// plus the `typed_value` tree) directly from `(full_column, type, row)` and its sub-columns,
    /// materializing only the genuinely per-row heterogeneous values (shared-variant blobs and cached
    /// full sparse columns) into transient arena columns. It handles every input.
    VariantColumnarTransientArena cursor_arena;
    const VariantColumnarCursorContext columnar_context{.dictionary = shared_context.dictionary, .format_settings = format_settings, .arena = &cursor_arena};

    auto encode_row = [&](size_t row)
    {
        metadata_string_column.insertData(shared_context.metadata.data(), shared_context.metadata.size());

        if (!shredded_type)
        {
            String encoded_value;
            bool ok = tryEncodeVariantColumnarValue(full_column, type, row, /* from_dynamic */ false, columnar_context, 1, encoded_value);
            if (!ok)
                throwCannotEncodeVariantColumnarValue(type);
            assert_cast<ColumnString &>(*value).insertData(encoded_value.data(), encoded_value.size());
            return;
        }

        VariantShreddedColumnarResult transformed;
        encodeVariantShreddedTypedValueColumnar(full_column, type, row, /* from_dynamic */ false, shredded_type, columnar_context, 1, transformed);

        auto & nullable_value = assert_cast<ColumnNullable &>(*value);
        auto & nested_value = assert_cast<ColumnString &>(nullable_value.getNestedColumn());
        auto & null_map = nullable_value.getNullMapData();
        if (!transformed.residual_value.has_value())
        {
            nested_value.insertDefault();
            null_map.push_back(UInt8(1));
        }
        else
        {
            nested_value.insertData(transformed.residual_value->data(), transformed.residual_value->size());
            null_map.push_back(UInt8(0));
        }
        typed_value->insert(transformed.typed_value.has_value() ? *transformed.typed_value : default_typed_value);
    };

    for (size_t row = 0; row < num_rows; ++row)
        encode_row(row);

    result.metadata_column = std::move(metadata);
    result.value_column = std::move(value);
    result.typed_value_column = std::move(typed_value);
    return result;
}

void analyzeVariantColumnForWrite(
    const IColumn & column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    VariantWriteAnalysisEntry & out_analysis)
{
    if (!out_analysis.source_type)
        out_analysis.source_type = type;

    ColumnPtr full_column_ptr = column.convertToFullColumnIfLowCardinality();
    VariantColumnarTransientArena arena;
    static const std::unordered_map<String, UInt32> empty_dictionary;
    const VariantColumnarCursorContext context{.dictionary = empty_dictionary, .format_settings = format_settings, .arena = &arena};
    const size_t num_rows = full_column_ptr->size();
    for (size_t row = 0; row < num_rows; ++row)
        analyzeVariantColumnarValue(*full_column_ptr, type, row, false, context, 1, &out_analysis.analysis, nullptr);
}

DataTypePtr inferVariantShreddedTypeForWrite(const VariantWriteAnalysisEntry & analysis)
{
    if (!analysis.source_type)
        return {};

    if (const auto * object_type = typeid_cast<const DataTypeObject *>(analysis.source_type.get()))
    {
        auto selected_paths = selectShreddedPathsForObject(analysis.analysis, *object_type);
        return constructShreddedType(analysis.analysis, &selected_paths);
    }

    return constructShreddedType(analysis.analysis);
}

}
