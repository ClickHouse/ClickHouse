#include <Processors/Formats/Impl/Parquet/VariantWrite.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>
#include <Processors/Formats/Impl/Parquet/UUIDUtils.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
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
#include <limits>
#include <map>
#include <tuple>
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

struct VariantTransformResult
{
    std::optional<String> residual_value;
    std::optional<Field> typed_value;
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

/// Keyed by `IDataType *` that stays alive for one `prepareVariantColumnsForWrite`
/// call. This cache is row-group local and must not outlive the encoding pass.
using VariantTransformScratch = std::unordered_map<const IDataType *, std::unordered_set<String>>;

struct VariantBuildStats
{
    std::unordered_set<String> * keys = nullptr;
    VariantWriteAnalysisNode * analysis = nullptr;
};

void addVariantAnalyzeScalarType(VariantWriteAnalysisNode & node, const DataTypePtr & type);
DataTypePtr getVariantAnalyzeScalarType(const Field & field, const DataTypePtr & type_hint);

VariantBuildStats makeVariantObjectChildStats(VariantBuildStats stats, const String & key)
{
    if (stats.keys)
        stats.keys->emplace(key);

    if (stats.analysis)
        stats.analysis = &stats.analysis->object_fields[key];

    return stats;
}

VariantBuildStats makeVariantArrayChildStats(VariantBuildStats stats)
{
    if (stats.analysis)
    {
        if (!stats.analysis->array_child)
            stats.analysis->array_child = std::make_unique<VariantWriteAnalysisNode>();
        stats.analysis = stats.analysis->array_child.get();
    }

    return stats;
}

template <typename BuildChild>
bool buildVariantArrayField(
    size_t size,
    VariantBuildStats stats,
    Field & out,
    BuildChild && build_child)
{
    if (stats.analysis)
    {
        ++stats.analysis->value_count;
        ++stats.analysis->array_count;
        if (!stats.analysis->array_child)
            stats.analysis->array_child = std::make_unique<VariantWriteAnalysisNode>();
    }

    Array result;
    result.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        Field child;
        if (!build_child(i, makeVariantArrayChildStats(stats), child))
            return false;

        result.emplace_back(std::move(child));
    }

    out = std::move(result);
    return true;
}

template <typename BuildChild>
bool buildVariantObjectField(
    size_t size,
    VariantBuildStats stats,
    Field & out,
    BuildChild && build_child)
{
    if (stats.analysis)
    {
        ++stats.analysis->value_count;
        ++stats.analysis->object_count;
    }

    Object result;
    for (size_t i = 0; i < size; ++i)
    {
        String key;
        Field child;
        if (!build_child(i, key, child))
            return false;

        if (!result.emplace(key, std::move(child)).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate key {} while preparing `Parquet` `VARIANT` object", key);
    }

    out = std::move(result);
    return true;
}

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

DataTypePtr getArrayChildTypeHint(const DataTypePtr & parent_type_hint, size_t index)
{
    DataTypePtr normalized_parent = unwrapVariantTypeHint(parent_type_hint);
    if (!normalized_parent)
        return nullptr;

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_parent.get()))
        return array_type->getNestedType();

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_parent.get()))
    {
        if (index < tuple_type->getElements().size())
            return tuple_type->getElement(index);
    }

    return nullptr;
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

template <typename T>
Field readVariantSharedNumberField(ReadBuffer & buf)
{
    T value = 0;
    readBinaryLittleEndian(value, buf);
    return Field(NearestFieldType<T>(value));
}

bool tryDeserializeVariantObjectSharedDataScalarFast(const ColumnString * shared_data_values, size_t index, Field & value)
{
    auto value_data = shared_data_values->getDataAt(index);
    ReadBufferFromMemory buf(value_data);

    UInt8 type_index = 0;
    readBinary(type_index, buf);
    switch (static_cast<BinaryTypeIndex>(type_index))
    {
        case BinaryTypeIndex::Nothing:
            value = Null();
            return true;
        case BinaryTypeIndex::Bool:
        {
            UInt8 raw = 0;
            readBinaryLittleEndian(raw, buf);
            value = Field(raw != 0);
            return true;
        }
        case BinaryTypeIndex::Int8:
            value = readVariantSharedNumberField<Int8>(buf);
            return true;
        case BinaryTypeIndex::Int16:
            value = readVariantSharedNumberField<Int16>(buf);
            return true;
        case BinaryTypeIndex::Int32:
        case BinaryTypeIndex::Date32:
            value = readVariantSharedNumberField<Int32>(buf);
            return true;
        case BinaryTypeIndex::Int64:
            value = readVariantSharedNumberField<Int64>(buf);
            return true;
        case BinaryTypeIndex::UInt8:
            value = readVariantSharedNumberField<UInt8>(buf);
            return true;
        case BinaryTypeIndex::UInt16:
        case BinaryTypeIndex::Date:
            value = readVariantSharedNumberField<UInt16>(buf);
            return true;
        case BinaryTypeIndex::UInt32:
            value = readVariantSharedNumberField<UInt32>(buf);
            return true;
        case BinaryTypeIndex::UInt64:
            value = readVariantSharedNumberField<UInt64>(buf);
            return true;
        case BinaryTypeIndex::Float32:
            value = readVariantSharedNumberField<Float32>(buf);
            return true;
        case BinaryTypeIndex::Float64:
            value = readVariantSharedNumberField<Float64>(buf);
            return true;
        case BinaryTypeIndex::String:
        {
            UInt64 size = 0;
            readVarUInt(size, buf);
            String str;
            str.resize(size);
            buf.readStrict(str.data(), size);
            value = std::move(str);
            return true;
        }
        default:
            return false;
    }
}

bool tryInsertVariantObjectFlatPath(
    Object & object,
    std::string_view path,
    Field value,
    const FormatSettings & format_settings,
    size_t depth)
{
    checkVariantWriteDepth(format_settings, depth);

    auto [head, tail] = Nested::splitName(path);
    String key = unescapeDotInJSONKey(String(head));

    if (tail.empty())
        return object.try_emplace(std::move(key), std::move(value)).second;

    auto [it, inserted] = object.try_emplace(key, Object{});
    if (!inserted && it->second.getType() != Field::Types::Object)
        return false;

    return tryInsertVariantObjectFlatPath(it->second.safeGet<Object>(), tail, std::move(value), format_settings, depth + 1);
}

bool tryBuildNestedObjectFieldFromColumnObject(
    const ColumnObject & object_column,
    size_t row,
    const FormatSettings & format_settings,
    size_t depth,
    Field & nested_object_field)
{
    checkVariantWriteDepth(format_settings, depth);

    Object nested_object;
    for (const auto & [path, column] : object_column.getTypedPaths())
    {
        if (!tryInsertVariantObjectFlatPath(nested_object, path, (*column)[row], format_settings, depth))
            return false;
    }

    for (const auto & [path, column] : object_column.getDynamicPathsPtrs())
    {
        if (!column->isNullAt(row) && !tryInsertVariantObjectFlatPath(nested_object, path, (*column)[row], format_settings, depth))
            return false;
    }

    const auto & shared_data_offsets = object_column.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
    size_t begin = row == 0 ? 0 : shared_data_offsets[row - 1];
    size_t end = shared_data_offsets[row];
    for (size_t i = begin; i != end; ++i)
    {
        if (!tryInsertVariantObjectFlatPath(
                nested_object,
                shared_data_paths->getDataAt(i),
                deserializeVariantObjectSharedDataValue(shared_data_values, i),
                format_settings,
                depth))
        {
            return false;
        }
    }

    nested_object_field = std::move(nested_object);
    return true;
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

bool getRelativeVariantObjectPath(
    std::string_view path,
    const std::vector<String> & prefix,
    std::vector<String> & relative_path)
{
    std::vector<String> segments = splitVariantObjectPath(path);
    if (segments.size() < prefix.size())
        return false;

    for (size_t i = 0; i < prefix.size(); ++i)
    {
        if (segments[i] != prefix[i])
            return false;
    }

    relative_path.assign(segments.begin() + static_cast<ssize_t>(prefix.size()), segments.end());
    return !relative_path.empty();
}

bool tryInsertVariantObjectPathSegments(Object & object, const std::vector<String> & segments, size_t pos, Field value)
{
    if (pos >= segments.size())
        return false;

    const String & key = segments[pos];
    if (pos + 1 == segments.size())
        return object.try_emplace(key, std::move(value)).second;

    auto [it, inserted] = object.try_emplace(key, Object{});
    if (!inserted && it->second.getType() != Field::Types::Object)
        return false;

    return tryInsertVariantObjectPathSegments(it->second.safeGet<Object>(), segments, pos + 1, std::move(value));
}

String variantObjectPathSegmentsToString(const std::vector<String> & segments)
{
    WriteBufferFromOwnString out;
    for (size_t i = 0; i < segments.size(); ++i)
    {
        if (i != 0)
            writeChar('.', out);
        writeString(segments[i], out);
    }
    return out.str();
}

void insertVariantObjectPathSegments(
    Object & object,
    const std::vector<String> & segments,
    Field value)
{
    if (!tryInsertVariantObjectPathSegments(object, segments, 0, std::move(value)))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot prepare `Object` path {} for `Parquet` `VARIANT` writing because it conflicts with another residual value",
            variantObjectPathSegmentsToString(segments));
}

bool shouldKeepVariantResidualPath(
    std::string_view path,
    const std::vector<String> & prefix,
    const std::unordered_set<String> & excluded_fields,
    std::vector<String> & relative_path)
{
    if (!getRelativeVariantObjectPath(path, prefix, relative_path))
        return false;

    return !excluded_fields.contains(relative_path.front());
}

struct VariantResidualPathInfo
{
    std::optional<std::vector<String>> relative_path;
    DataTypePtr type_hint;
    bool type_hint_initialized = false;
};

DataTypePtr getObjectChildTypeHint(
    const DataTypePtr & parent_type_hint,
    const DataTypeObject * object_type,
    std::string_view child_path,
    std::string_view child_name);

DataTypePtr getVariantObjectLeafTypeHint(
    const DataTypePtr & object_type,
    const DataTypeObject * object_data_type,
    std::string_view full_path,
    const std::vector<String> & relative_path);

struct VariantResidualPathCache
{
    const std::vector<String> & prefix;
    const std::unordered_set<String> & excluded_fields;
    const FormatSettings & format_settings;
    std::unordered_map<std::string_view, VariantResidualPathInfo> cache;

    VariantResidualPathInfo * getInfo(std::string_view path)
    {
        auto [it, inserted] = cache.try_emplace(path);
        if (inserted)
        {
            std::vector<String> parsed_path;
            if (shouldKeepVariantResidualPath(it->first, prefix, excluded_fields, parsed_path))
            {
                checkVariantWriteDepth(format_settings, prefix.size() + parsed_path.size() + 1);
                it->second.relative_path = std::move(parsed_path);
            }
        }

        return &it->second;
    }

    const std::vector<String> * get(std::string_view path)
    {
        VariantResidualPathInfo * info = getInfo(path);
        if (!info->relative_path.has_value())
            return nullptr;

        return &*info->relative_path;
    }

    const VariantResidualPathInfo * getResidualInfo(
        std::string_view path,
        const DataTypePtr & object_type,
        const DataTypeObject * object_data_type)
    {
        VariantResidualPathInfo * info = getInfo(path);
        if (!info->relative_path.has_value())
            return nullptr;

        if (!info->type_hint_initialized)
        {
            info->type_hint = getVariantObjectLeafTypeHint(object_type, object_data_type, path, *info->relative_path);
            info->type_hint_initialized = true;
        }

        return info;
    }
};

size_t getVariantScalarEncodedSize(const Field & field, const DataTypePtr & type_hint);
void writeVariantScalarToBuffer(const Field & field, const DataTypePtr & type_hint, char *& out);
size_t getVariantEncodedStringSize(std::string_view value);
void writeVariantStringPayloadToBuffer(std::string_view value, char *& out);
template <typename T>
void writeVariantPODToBuffer(T value, char *& out);
void writeVariantLittleEndianToBuffer(UInt64 value, UInt8 size, char *& out);

enum class DirectVariantResidualValueKind
{
    Field,
    Null,
    Bool,
    Int64,
    UInt64,
    Float64,
    StringView,
};

struct DirectVariantResidualEntry
{
    const std::vector<String> * path = nullptr;
    DirectVariantResidualValueKind value_kind = DirectVariantResidualValueKind::Field;
    Field value;
    bool bool_value = false;
    Int64 int64_value = 0;
    UInt64 uint64_value = 0;
    Float64 float64_value = 0;
    std::string_view string_value;
    DataTypePtr type_hint;
    size_t value_size = 0;
};

size_t getDirectVariantResidualScalarEncodedSize(const DirectVariantResidualEntry & entry);
void writeDirectVariantResidualEntryToBuffer(const DirectVariantResidualEntry & entry, char *& out);

struct DirectVariantResidualColumn
{
    const IColumn * column = nullptr;
    const std::vector<String> * path = nullptr;
    DataTypePtr type_hint;
    bool skip_nulls = false;
};

struct DirectVariantSharedPathSlot
{
    std::string_view path;
    const VariantResidualPathInfo * info = nullptr;
    bool initialized = false;
};

struct DirectVariantObjectEncoding;

struct DirectVariantObjectChild
{
    UInt32 field_id = 0;
    size_t child_size = 0;
    size_t entry_index = 0;
    DirectVariantObjectEncoding * nested = nullptr;
};

struct DirectVariantObjectEncoding
{
    UInt8 field_id_size = 0;
    UInt8 field_offset_size = 0;
    bool is_large = false;
    size_t total_children_size = 0;
    size_t total_size = 0;
    std::vector<DirectVariantObjectChild> children;
};

struct DirectVariantObjectEncodingScratch
{
    DirectVariantObjectEncoding root;
    std::vector<std::unique_ptr<DirectVariantObjectEncoding>> nested_encodings;
    size_t next_nested_encoding = 0;

    void reset()
    {
        next_nested_encoding = 0;
    }

    DirectVariantObjectEncoding & acquireNested()
    {
        if (next_nested_encoding == nested_encodings.size())
            nested_encodings.emplace_back(std::make_unique<DirectVariantObjectEncoding>());

        return *nested_encodings[next_nested_encoding++];
    }
};

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

bool isVariantDirectResidualScalar(const Field & value)
{
    return value.getType() != Field::Types::Object && value.getType() != Field::Types::Array;
}

DataTypePtr getVariantObjectLeafTypeHint(
    const DataTypePtr & object_type,
    const DataTypeObject * object_data_type,
    std::string_view full_path,
    const std::vector<String> & relative_path)
{
    if (relative_path.empty())
        return nullptr;

    return getObjectChildTypeHint(object_type, object_data_type, full_path, relative_path.back());
}

bool measureDirectVariantObjectEncoding(
    const std::vector<DirectVariantResidualEntry> & entries,
    size_t begin,
    size_t end,
    size_t depth,
    size_t base_depth,
    const FormatSettings & format_settings,
    const std::unordered_map<String, UInt32> & dictionary,
    DirectVariantObjectEncodingScratch & scratch,
    DirectVariantObjectEncoding & encoding)
{
    checkVariantWriteDepth(format_settings, base_depth + depth);

    encoding.total_children_size = 0;
    encoding.total_size = 0;
    encoding.children.clear();
    encoding.children.reserve(end - begin);

    UInt32 highest_field_id = 0;
    for (size_t pos = begin; pos < end;)
    {
        const auto & path = *entries[pos].path;
        if (depth >= path.size())
            return false;

        const String & key = path[depth];
        size_t next = pos + 1;
        while (next < end && (*entries[next].path)[depth] == key)
            ++next;

        auto dictionary_it = dictionary.find(key);
        if (dictionary_it == dictionary.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing `Parquet` `VARIANT` dictionary entry for key {}", key);

        DirectVariantObjectChild child;
        child.field_id = dictionary_it->second;
        highest_field_id = std::max(highest_field_id, child.field_id);

        if (path.size() == depth + 1)
        {
            if (next != pos + 1)
                return false;

            child.entry_index = pos;
            child.child_size = entries[pos].value_size;
        }
        else
        {
            child.nested = &scratch.acquireNested();
            if (!measureDirectVariantObjectEncoding(entries, pos, next, depth + 1, base_depth, format_settings, dictionary, scratch, *child.nested))
                return false;

            child.child_size = child.nested->total_size;
        }

        addVariantEncodingSizeOrThrow(encoding.total_children_size, child.child_size, "`object` children");
        encoding.children.emplace_back(std::move(child));
        pos = next;
    }

    finishVariantObjectEncodingHeader(
        encoding.children.size(),
        highest_field_id,
        encoding.total_children_size,
        encoding.field_id_size,
        encoding.field_offset_size,
        encoding.is_large,
        encoding.total_size);
    return true;
}

void writeDirectVariantObjectToBuffer(
    const std::vector<DirectVariantResidualEntry> & entries,
    const DirectVariantObjectEncoding & encoding,
    char *& out)
{
    writeVariantObjectHeaderAndChildren(
        encoding.children,
        encoding.field_id_size,
        encoding.field_offset_size,
        encoding.is_large,
        out,
        [&](const DirectVariantObjectChild & child)
        {
            if (child.nested)
            {
                writeDirectVariantObjectToBuffer(entries, *child.nested, out);
            }
            else
            {
                const auto & entry = entries[child.entry_index];
                writeDirectVariantResidualEntryToBuffer(entry, out);
            }
        });
}

bool addDirectVariantResidualEntry(
    std::vector<DirectVariantResidualEntry> & entries,
    const std::vector<String> * relative_path,
    DataTypePtr type_hint,
    Field value)
{
    if (!relative_path || !isVariantDirectResidualScalar(value))
        return false;

    DirectVariantResidualEntry entry;
    entry.path = relative_path;
    entry.value = std::move(value);
    entry.type_hint = std::move(type_hint);
    entry.value_size = getVariantScalarEncodedSize(entry.value, entry.type_hint);
    entries.emplace_back(std::move(entry));
    return true;
}

template <typename T>
T readVariantSharedNumberValue(ReadBuffer & buf)
{
    T value = 0;
    readBinaryLittleEndian(value, buf);
    return value;
}

bool addDirectVariantResidualSharedDataScalarEntry(
    std::vector<DirectVariantResidualEntry> & entries,
    const std::vector<String> * relative_path,
    DataTypePtr type_hint,
    const ColumnString * shared_data_values,
    size_t index)
{
    if (!relative_path)
        return false;

    if (type_hint)
    {
        Field value;
        if (!tryDeserializeVariantObjectSharedDataScalarFast(shared_data_values, index, value))
            value = deserializeVariantObjectSharedDataValue(shared_data_values, index);

        return addDirectVariantResidualEntry(entries, relative_path, std::move(type_hint), std::move(value));
    }

    auto value_data = shared_data_values->getDataAt(index);
    ReadBufferFromMemory buf(value_data);

    DirectVariantResidualEntry entry;
    entry.path = relative_path;

    UInt8 type_index = 0;
    readBinary(type_index, buf);
    switch (static_cast<BinaryTypeIndex>(type_index))
    {
        case BinaryTypeIndex::Nothing:
            entry.value_kind = DirectVariantResidualValueKind::Null;
            break;
        case BinaryTypeIndex::Bool:
            entry.value_kind = DirectVariantResidualValueKind::Bool;
            entry.bool_value = readVariantSharedNumberValue<UInt8>(buf) != 0;
            break;
        case BinaryTypeIndex::Int8:
            entry.value_kind = DirectVariantResidualValueKind::Int64;
            entry.int64_value = readVariantSharedNumberValue<Int8>(buf);
            break;
        case BinaryTypeIndex::Int16:
            entry.value_kind = DirectVariantResidualValueKind::Int64;
            entry.int64_value = readVariantSharedNumberValue<Int16>(buf);
            break;
        case BinaryTypeIndex::Int32:
        case BinaryTypeIndex::Date32:
            entry.value_kind = DirectVariantResidualValueKind::Int64;
            entry.int64_value = readVariantSharedNumberValue<Int32>(buf);
            break;
        case BinaryTypeIndex::Int64:
            entry.value_kind = DirectVariantResidualValueKind::Int64;
            entry.int64_value = readVariantSharedNumberValue<Int64>(buf);
            break;
        case BinaryTypeIndex::UInt8:
            entry.value_kind = DirectVariantResidualValueKind::UInt64;
            entry.uint64_value = readVariantSharedNumberValue<UInt8>(buf);
            break;
        case BinaryTypeIndex::UInt16:
        case BinaryTypeIndex::Date:
            entry.value_kind = DirectVariantResidualValueKind::UInt64;
            entry.uint64_value = readVariantSharedNumberValue<UInt16>(buf);
            break;
        case BinaryTypeIndex::UInt32:
            entry.value_kind = DirectVariantResidualValueKind::UInt64;
            entry.uint64_value = readVariantSharedNumberValue<UInt32>(buf);
            break;
        case BinaryTypeIndex::UInt64:
            entry.value_kind = DirectVariantResidualValueKind::UInt64;
            entry.uint64_value = readVariantSharedNumberValue<UInt64>(buf);
            break;
        case BinaryTypeIndex::Float32:
            entry.value_kind = DirectVariantResidualValueKind::Float64;
            entry.float64_value = static_cast<Float64>(readVariantSharedNumberValue<Float32>(buf));
            break;
        case BinaryTypeIndex::Float64:
            entry.value_kind = DirectVariantResidualValueKind::Float64;
            entry.float64_value = readVariantSharedNumberValue<Float64>(buf);
            break;
        case BinaryTypeIndex::String:
        {
            size_t size = 0;
            readVarUInt(size, buf);
            if (unlikely(size > DEFAULT_MAX_STRING_SIZE))
                throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size.");

            entry.value_kind = DirectVariantResidualValueKind::StringView;
            entry.string_value = std::string_view(buf.position(), size);
            buf.ignore(size);
            break;
        }
        default:
            return false;
    }

    entry.value_size = getDirectVariantResidualScalarEncodedSize(entry);
    entries.emplace_back(std::move(entry));
    return true;
}

bool directVariantResidualEntryLess(const DirectVariantResidualEntry & left, const DirectVariantResidualEntry & right)
{
    return std::lexicographical_compare(
        left.path->begin(),
        left.path->end(),
        right.path->begin(),
        right.path->end());
}

const VariantResidualPathInfo * getCachedDirectVariantSharedPathInfo(
    std::string_view path,
    const DataTypePtr & object_type,
    const DataTypeObject * object_data_type,
    VariantResidualPathCache & path_cache,
    DirectVariantSharedPathSlot & slot)
{
    if (!slot.initialized || slot.path != path)
    {
        slot.path = path;
        slot.info = path_cache.getResidualInfo(path, object_type, object_data_type);
        slot.initialized = true;
    }

    return slot.info;
}

bool tryEncodeDirectVariantObjectResidualForRow(
    const ColumnObject & object_column,
    size_t row,
    const DataTypePtr & object_type,
    const DataTypeObject * object_data_type,
    VariantResidualPathCache & path_cache,
    const std::unordered_map<String, UInt32> & dictionary,
    const std::vector<DirectVariantResidualColumn> & residual_columns,
    std::vector<DirectVariantResidualEntry> & entries,
    std::vector<DirectVariantSharedPathSlot> & shared_path_slots,
    DirectVariantObjectEncodingScratch & encoding_scratch,
    std::optional<String> & out)
{
    entries.clear();

    for (const auto & residual_column : residual_columns)
    {
        if (residual_column.skip_nulls && residual_column.column->isNullAt(row))
            continue;

        if (!addDirectVariantResidualEntry(
                entries,
                residual_column.path,
                residual_column.type_hint,
                (*residual_column.column)[row]))
        {
            return false;
        }
    }

    const auto & shared_data_offsets = object_column.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
    size_t begin = row == 0 ? 0 : shared_data_offsets[row - 1];
    size_t end = shared_data_offsets[row];
    entries.reserve(entries.size() + end - begin);
    for (size_t i = begin; i != end; ++i)
    {
        auto full_path = shared_data_paths->getDataAt(i);
        size_t slot_index = i - begin;
        if (slot_index >= shared_path_slots.size())
            shared_path_slots.resize(slot_index + 1);

        const auto * info = getCachedDirectVariantSharedPathInfo(
            full_path,
            object_type,
            object_data_type,
            path_cache,
            shared_path_slots[slot_index]);
        if (!info)
            continue;

        if (!addDirectVariantResidualSharedDataScalarEntry(
                entries,
                &*info->relative_path,
                info->type_hint,
                shared_data_values,
                i))
        {
            return false;
        }
    }

    if (entries.empty())
    {
        out = std::nullopt;
        return true;
    }

    if (!std::is_sorted(entries.begin(), entries.end(), directVariantResidualEntryLess))
        std::sort(entries.begin(), entries.end(), directVariantResidualEntryLess);

    encoding_scratch.reset();
    if (!measureDirectVariantObjectEncoding(
            entries,
            0,
            entries.size(),
            0,
            path_cache.prefix.size() + 1,
            path_cache.format_settings,
            dictionary,
            encoding_scratch,
            encoding_scratch.root))
        return false;

    out = String(encoding_scratch.root.total_size, '\0');
    char * out_pos = out->data();
    writeDirectVariantObjectToBuffer(entries, encoding_scratch.root, out_pos);
    chassert(out_pos == out->data() + out->size());
    return true;
}

Object buildVariantObjectResidualForRow(
    const ColumnObject & object_column,
    size_t row,
    VariantResidualPathCache & path_cache)
{
    Object residual;

    for (const auto & [path, column] : object_column.getTypedPaths())
    {
        if (const auto * relative_path = path_cache.get(path))
            insertVariantObjectPathSegments(residual, *relative_path, (*column)[row]);
    }

    for (const auto & [path, column] : object_column.getDynamicPathsPtrs())
    {
        if (!column->isNullAt(row))
        {
            if (const auto * relative_path = path_cache.get(path))
                insertVariantObjectPathSegments(residual, *relative_path, (*column)[row]);
        }
    }

    const auto & shared_data_offsets = object_column.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
    size_t begin = row == 0 ? 0 : shared_data_offsets[row - 1];
    size_t end = shared_data_offsets[row];
    for (size_t i = begin; i != end; ++i)
    {
        if (const auto * relative_path = path_cache.get(shared_data_paths->getDataAt(i)))
        {
            insertVariantObjectPathSegments(
                residual,
                *relative_path,
                deserializeVariantObjectSharedDataValue(shared_data_values, i));
        }
    }

    return residual;
}

std::unordered_set<String> getVariantTupleFieldNames(const DataTypeTuple & tuple_type)
{
    std::unordered_set<String> field_names;
    field_names.reserve(tuple_type.getElements().size());
    for (size_t i = 0; i < tuple_type.getElements().size(); ++i)
        field_names.emplace(tuple_type.getNameByPosition(i + 1));

    return field_names;
}

void encodeVariantObject(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    std::optional<String> & out,
    const std::unordered_set<String> * excluded_fields);

void normalizeVariantFieldForUntypedResidual(
    Field & field,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const FormatSettings & format_settings,
    size_t depth);

MutableColumnPtr buildVariantResidualValueColumnForObject(
    const ColumnObject & object_column,
    const DataTypePtr & object_type,
    const std::vector<String> & prefix,
    std::string_view prefix_path,
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
    VariantResidualPathCache path_cache
    {
        .prefix = prefix,
        .excluded_fields = excluded_fields,
        .format_settings = format_settings,
        .cache = {},
    };
    path_cache.cache.reserve(object_column.getTypedPaths().size() + object_column.getDynamicPathsPtrs().size() + 32);

    std::vector<DirectVariantResidualColumn> direct_residual_columns;
    direct_residual_columns.reserve(object_column.getTypedPaths().size() + object_column.getDynamicPathsPtrs().size());
    for (const auto & [path, column] : object_column.getTypedPaths())
    {
        if (const auto * info = path_cache.getResidualInfo(path, object_type, object_data_type))
        {
            direct_residual_columns.emplace_back(DirectVariantResidualColumn
            {
                .column = column.get(),
                .path = &*info->relative_path,
                .type_hint = info->type_hint,
                .skip_nulls = false,
            });
        }
    }

    for (const auto & [path, column] : object_column.getDynamicPathsPtrs())
    {
        if (const auto * info = path_cache.getResidualInfo(path, object_type, object_data_type))
        {
            direct_residual_columns.emplace_back(DirectVariantResidualColumn
            {
                .column = column,
                .path = &*info->relative_path,
                .type_hint = info->type_hint,
                .skip_nulls = true,
            });
        }
    }

    std::vector<DirectVariantResidualEntry> direct_entries;
    direct_entries.reserve(direct_residual_columns.size());
    std::vector<DirectVariantSharedPathSlot> shared_path_slots;
    DirectVariantObjectEncodingScratch direct_encoding_scratch;

    for (size_t row = 0; row < object_column.size(); ++row)
    {
        std::optional<String> encoded_value;
        if (tryEncodeDirectVariantObjectResidualForRow(
                object_column,
                row,
                object_type,
                object_data_type,
                path_cache,
                context.dictionary,
                direct_residual_columns,
                direct_entries,
                shared_path_slots,
                direct_encoding_scratch,
                encoded_value))
        {
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
            continue;
        }

        Object residual = buildVariantObjectResidualForRow(object_column, row, path_cache);
        if (residual.empty())
        {
            nested_value.insertDefault();
            null_map.push_back(UInt8(1));
            continue;
        }

        Field residual_field(std::move(residual));
        normalizeVariantFieldForUntypedResidual(residual_field, object_type, object_data_type, prefix_path, format_settings, prefix.size() + 1);
        encodeVariantObject(residual_field, object_type, object_data_type, prefix_path, context.dictionary, encoded_value, nullptr);
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
            path,
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

template <typename T>
bool tryConvertIntegralFieldValue(const Field & field, T & value)
{
    switch (field.getType())
    {
        case Field::Types::Int64:
        {
            Int64 source = field.safeGet<Int64>();
            if constexpr (std::numeric_limits<T>::is_signed)
            {
                if (source < static_cast<Int64>(std::numeric_limits<T>::min()) || source > static_cast<Int64>(std::numeric_limits<T>::max()))
                    return false;
            }
            else
            {
                if (source < 0 || static_cast<UInt64>(source) > static_cast<UInt64>(std::numeric_limits<T>::max()))
                    return false;
            }
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::UInt64:
        {
            UInt64 source = field.safeGet<UInt64>();
            if (source > static_cast<UInt64>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::Int128:
        {
            Int128 source = field.safeGet<Int128>();
            if constexpr (std::numeric_limits<T>::is_signed)
            {
                if (source < static_cast<Int128>(std::numeric_limits<T>::min()) || source > static_cast<Int128>(std::numeric_limits<T>::max()))
                    return false;
            }
            else
            {
                if (source < 0 || static_cast<UInt128>(source) > static_cast<UInt128>(std::numeric_limits<T>::max()))
                    return false;
            }
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::UInt128:
        {
            UInt128 source = field.safeGet<UInt128>();
            if (source > static_cast<UInt128>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::Int256:
        {
            Int256 source = field.safeGet<Int256>();
            if constexpr (std::numeric_limits<T>::is_signed)
            {
                if (source < static_cast<Int256>(std::numeric_limits<T>::min()) || source > static_cast<Int256>(std::numeric_limits<T>::max()))
                    return false;
            }
            else
            {
                if (source < 0 || static_cast<UInt256>(source) > static_cast<UInt256>(std::numeric_limits<T>::max()))
                    return false;
            }
            value = static_cast<T>(source);
            return true;
        }
        case Field::Types::UInt256:
        {
            UInt256 source = field.safeGet<UInt256>();
            if (source > static_cast<UInt256>(std::numeric_limits<T>::max()))
                return false;
            value = static_cast<T>(source);
            return true;
        }
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

DataTypePtr getObjectChildTypeHint(
    const DataTypePtr & parent_type_hint,
    const DataTypeObject * object_type,
    std::string_view child_path,
    std::string_view child_name)
{
    if (object_type)
    {
        const auto & typed_paths = object_type->getTypedPaths();
        auto it = typed_paths.find(String(child_path));
        if (it != typed_paths.end())
            return it->second;
    }

    DataTypePtr normalized_parent = unwrapVariantTypeHint(parent_type_hint);
    if (!normalized_parent)
        return nullptr;

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_parent.get()))
    {
        if (!tuple_type->hasExplicitNames())
            return nullptr;

        auto position = tuple_type->tryGetPositionByName(child_name);
        if (!position.has_value())
            return nullptr;

        return tuple_type->getElement(*position);
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(normalized_parent.get()))
        return map_type->getValueType();

    return nullptr;
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

bool buildVariantField(
    const Field & field,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out);

bool buildVariantFieldFromColumn(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out,
    DataTypePtr * out_value_type_hint = nullptr);

bool buildVariantFieldFromDynamicSharedVariant(
    const ColumnDynamic & dynamic_column,
    size_t row,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out,
    DataTypePtr * out_value_type_hint)
{
    const auto & variant_column = dynamic_column.getVariantColumn();
    const auto value = dynamic_column.getSharedVariant().getDataAt(variant_column.offsetAt(row));

    ReadBufferFromMemory buf(value);
    auto nested_type = decodeDataType(buf);
    auto nested_column = nested_type->createColumn();
    nested_type->getDefaultSerialization()->deserializeBinary(*nested_column, buf, FormatSettings());

    return buildVariantFieldFromColumn(*nested_column, nested_type, 0, format_settings, depth + 1, stats, out, out_value_type_hint);
}

bool buildVariantField(
    const Field & field,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out)
{
    checkVariantWriteDepth(format_settings, depth);

    DataTypePtr normalized_type_hint = unwrapVariantTypeHint(type_hint);

    switch (field.getType())
    {
        case Field::Types::Object:
        {
            const auto & object = field.safeGet<Object>();
            auto it = object.begin();
            return buildVariantObjectField(
                object.size(),
                stats,
                out,
                [&](size_t, String & key, Field & child)
                {
                    const auto & [object_key, object_value] = *it;
                    ++it;
                    key = object_key;
                    String child_path = appendVariantJSONPath(current_path, key);
                    return buildVariantField(
                        object_value,
                        getObjectChildTypeHint(normalized_type_hint, object_type, child_path, key),
                        object_type,
                        child_path,
                        format_settings,
                        depth + 1,
                        makeVariantObjectChildStats(stats, key),
                        child);
                });
        }
        case Field::Types::Array:
        {
            const auto & array = field.safeGet<Array>();
            return buildVariantArrayField(
                array.size(),
                stats,
                out,
                [&](size_t i, VariantBuildStats child_stats, Field & child)
                {
                    return buildVariantField(
                        array[i],
                        getArrayChildTypeHint(normalized_type_hint, i),
                        object_type,
                        current_path,
                        format_settings,
                        depth + 1,
                        child_stats,
                        child);
                });
        }
        case Field::Types::Tuple:
        {
            const auto & tuple = field.safeGet<Tuple>();
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_type_hint.get());
            if (tuple_type && tuple_type->hasExplicitNames())
            {
                return buildVariantObjectField(
                    tuple.size(),
                    stats,
                    out,
                    [&](size_t i, String & key, Field & child)
                    {
                        key = tuple_type->getNameByPosition(i + 1);
                        String child_path = appendVariantJSONPath(current_path, key);
                        return buildVariantField(
                            tuple[i],
                            tuple_type->getElement(i),
                            object_type,
                            child_path,
                            format_settings,
                            depth + 1,
                            makeVariantObjectChildStats(stats, key),
                            child);
                    });
            }

            return buildVariantArrayField(
                tuple.size(),
                stats,
                out,
                [&](size_t i, VariantBuildStats child_stats, Field & child)
                {
                    return buildVariantField(
                        tuple[i],
                        getArrayChildTypeHint(normalized_type_hint, i),
                        object_type,
                        current_path,
                        format_settings,
                        depth + 1,
                        child_stats,
                        child);
                });
        }
        case Field::Types::Map:
        {
            const auto & map = field.safeGet<Map>();
            return buildVariantObjectField(
                map.size(),
                stats,
                out,
                [&](size_t i, String & key, Field & child)
                {
                    const auto & entry = map[i];
                    if (entry.getType() != Field::Types::Tuple)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected `Map` entry type {} while preparing `Parquet` `VARIANT`", entry.getTypeName());

                    const auto & tuple = entry.safeGet<Tuple>();
                    if (tuple.size() != 2)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected `Map` entry size {} while preparing `Parquet` `VARIANT`", tuple.size());

                    if (tuple[0].getType() != Field::Types::String)
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `Map(String, T)` can be written as `Parquet` `VARIANT`");

                    key = tuple[0].safeGet<String>();
                    String child_path = appendVariantJSONPath(current_path, key);
                    return buildVariantField(
                        tuple[1],
                        getObjectChildTypeHint(normalized_type_hint, object_type, child_path, key),
                        object_type,
                        child_path,
                        format_settings,
                        depth + 1,
                        makeVariantObjectChildStats(stats, key),
                        child);
                });
        }
        default:
            if (object_type)
            {
                if (auto normalized_field = tryNormalizeVariantTemporalScalarToString(
                        field,
                        normalized_type_hint,
                        format_settings,
                        [](const DataTypePtr & normalized_type)
                        {
                            return isTime(normalized_type) || isTime64(normalized_type);
                        }))
                {
                    out = std::move(*normalized_field);
                    normalized_type_hint = getVariantStringType();
                }
                else
                {
                    out = field;
                }
            }
            else
            {
                out = field;
            }

            if (stats.analysis)
            {
                ++stats.analysis->value_count;
                if (auto scalar_type = getVariantAnalyzeScalarType(out, normalized_type_hint))
                    addVariantAnalyzeScalarType(*stats.analysis, scalar_type);
            }
            return true;
    }
}

bool buildVariantFieldFromColumn(
    const IColumn & column,
    const DataTypePtr & type,
    size_t row,
    const FormatSettings & format_settings,
    size_t depth,
    VariantBuildStats stats,
    Field & out,
    DataTypePtr * out_value_type_hint)
{
    checkVariantWriteDepth(format_settings, depth);

    DataTypePtr normalized_type = unwrapVariantTypeHint(type);
    if (typeid_cast<const DataTypeDynamic *>(normalized_type.get()))
    {
        const auto * dynamic_column = typeid_cast<const ColumnDynamic *>(&column);
        if (!dynamic_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected `ColumnDynamic` while preparing nested `Dynamic` value for `Parquet` `VARIANT`");

        auto nested_type = dynamic_column->getTypeAt(row);
        if (!nested_type)
        {
            if (stats.analysis)
                ++stats.analysis->value_count;
            if (out_value_type_hint)
                out_value_type_hint->reset();
            out = Field();
            return true;
        }

        const auto & variant_column = dynamic_column->getVariantColumn();
        auto discr = variant_column.globalDiscriminatorAt(row);
        if (discr != dynamic_column->getSharedVariantDiscriminator())
        {
            const auto & nested_column = variant_column.getVariantByGlobalDiscriminator(discr);
            return buildVariantFieldFromColumn(nested_column, nested_type, variant_column.offsetAt(row), format_settings, depth + 1, stats, out, out_value_type_hint);
        }

        return buildVariantFieldFromDynamicSharedVariant(*dynamic_column, row, format_settings, depth, stats, out, out_value_type_hint);
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_type.get()))
    {
        const auto * tuple_column = typeid_cast<const ColumnTuple *>(&column);
        if (!tuple_column)
        {
            if (out_value_type_hint)
                *out_value_type_hint = normalized_type;
            return buildVariantField(column[row], normalized_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
        }

        if (out_value_type_hint)
            *out_value_type_hint = normalized_type;

        if (tuple_type->hasExplicitNames())
        {
            return buildVariantObjectField(
                tuple_type->getElements().size(),
                stats,
                out,
                [&](size_t i, String & key, Field & child)
                {
                    key = tuple_type->getNameByPosition(i + 1);
                    return buildVariantFieldFromColumn(
                        tuple_column->getColumn(i),
                        tuple_type->getElement(i),
                        row,
                        format_settings,
                        depth + 1,
                        makeVariantObjectChildStats(stats, key),
                        child);
                });
        }

        return buildVariantArrayField(
            tuple_type->getElements().size(),
            stats,
            out,
            [&](size_t i, VariantBuildStats child_stats, Field & child)
            {
                return buildVariantFieldFromColumn(
                    tuple_column->getColumn(i),
                    tuple_type->getElement(i),
                    row,
                    format_settings,
                    depth + 1,
                    child_stats,
                    child);
            });
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_type.get()))
    {
        const auto * array_column = typeid_cast<const ColumnArray *>(&column);
        if (!array_column)
        {
            if (out_value_type_hint)
                *out_value_type_hint = normalized_type;
            return buildVariantField(column[row], normalized_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
        }

        if (out_value_type_hint)
            *out_value_type_hint = normalized_type;

        const auto & offsets = array_column->getOffsets();
        size_t begin = row == 0 ? 0 : offsets[row - 1];
        size_t end = offsets[row];
        return buildVariantArrayField(
            end - begin,
            stats,
            out,
            [&](size_t i, VariantBuildStats child_stats, Field & child)
            {
                return buildVariantFieldFromColumn(
                    array_column->getData(),
                    array_type->getNestedType(),
                    begin + i,
                    format_settings,
                    depth + 1,
                    child_stats,
                    child);
            });
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(normalized_type.get()))
    {
        const auto * map_column = typeid_cast<const ColumnMap *>(&column);
        if (!map_column)
        {
            if (out_value_type_hint)
                *out_value_type_hint = normalized_type;
            return buildVariantField(column[row], normalized_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
        }

        if (out_value_type_hint)
            *out_value_type_hint = normalized_type;

        const auto & offsets = map_column->getNestedColumn().getOffsets();
        size_t begin = row == 0 ? 0 : offsets[row - 1];
        size_t end = offsets[row];
        const auto & nested_data = map_column->getNestedData();
        const auto & keys_column = *nested_data.getColumnPtr(0);
        const auto & values_column = *nested_data.getColumnPtr(1);
        return buildVariantObjectField(
            end - begin,
            stats,
            out,
            [&](size_t i, String & key, Field & child)
            {
                Field key_field = keys_column[begin + i];
                if (key_field.getType() != Field::Types::String)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `Map(String, T)` can be written as `Parquet` `VARIANT`");

                key = key_field.safeGet<String>();
                return buildVariantFieldFromColumn(
                    values_column,
                    map_type->getValueType(),
                    begin + i,
                    format_settings,
                    depth + 1,
                    makeVariantObjectChildStats(stats, key),
                    child);
            });
    }

    if (out_value_type_hint)
        *out_value_type_hint = normalized_type;
    return buildVariantField(column[row], normalized_type, nullptr, std::string_view{}, format_settings, depth, stats, out);
}

void normalizeVariantFieldForUntypedResidual(
    Field & field,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const FormatSettings & format_settings,
    size_t depth)
{
    checkVariantWriteDepth(format_settings, depth);

    DataTypePtr normalized_type_hint = unwrapVariantTypeHint(type_hint);

    if (field.getType() == Field::Types::Object)
    {
        auto & object = field.safeGet<Object>();
        for (auto & [key, child] : object)
        {
            String child_path = appendVariantJSONPath(current_path, key);
            normalizeVariantFieldForUntypedResidual(
                child,
                getObjectChildTypeHint(normalized_type_hint, object_type, child_path, key),
                object_type,
                child_path,
                format_settings,
                depth + 1);
        }
        return;
    }

    if (field.getType() == Field::Types::Array)
    {
        auto & array = field.safeGet<Array>();
        for (size_t i = 0; i < array.size(); ++i)
        {
            normalizeVariantFieldForUntypedResidual(
                array[i],
                getArrayChildTypeHint(normalized_type_hint, i),
                object_type,
                current_path,
                format_settings,
                depth + 1);
        }
        return;
    }

    if (auto normalized_field = tryNormalizeVariantTemporalScalarToString(
            field,
            normalized_type_hint,
            format_settings,
            [](const DataTypePtr & normalized_type)
            {
                return isDateOrDate32(normalized_type)
                    || isDateTimeOrDateTime64(normalized_type)
                    || isTime(normalized_type)
                    || isTime64(normalized_type);
            }))
    {
        field = std::move(*normalized_field);
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

template <typename Sink, typename T>
void writeVariantDecimalPrimitive(VariantPrimitiveType type, const DecimalField<T> & value, Sink & sink)
{
    sink.writePrimitiveHeader(type);
    sink.writePOD(static_cast<UInt8>(value.getScale()));
    sink.writePOD(value.getValue());
}

template <typename Sink>
bool tryEncodeVariantScalarUsingTypeHint(const Field & field, const DataTypePtr & type_hint, Sink & sink)
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
            if (!tryConvertIntegralFieldValue(field, converted) || converted > static_cast<UInt64>(std::numeric_limits<Int32>::max()))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Date, static_cast<Int32>(converted), sink);
            return true;
        }
        case TypeIndex::Date32:
        {
            Int32 converted = 0;
            if (!tryConvertIntegralFieldValue(field, converted))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Date, converted, sink);
            return true;
        }
        case TypeIndex::DateTime:
        {
            UInt64 converted = 0;
            if (!tryConvertIntegralFieldValue(field, converted) || converted > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return false;

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(static_cast<Int64>(converted), 0, 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `DateTime` value as `Parquet` `VARIANT` timestamp");

            const auto & date_time_type = assert_cast<const DataTypeDateTime &>(*normalized_type);
            /// `DateTime` with an explicit time zone is an instant in time, so it maps to the
            /// adjusted-to-UTC `VARIANT` timestamp. Plain `DateTime` keeps "wall clock" semantics
            /// and uses the NTZ timestamp tag instead.
            writeVariantSignedIntegralPrimitive(
                date_time_type.hasExplicitTimeZone() ? VariantPrimitiveType::TimestampMicros : VariantPrimitiveType::TimestampNtzMicros,
                micros,
                sink);
            return true;
        }
        case TypeIndex::DateTime64:
        {
            if (field.getType() != Field::Types::Decimal64)
                return false;

            const auto & decimal = field.safeGet<DecimalField<DateTime64>>();
            const auto & date_time_type = assert_cast<const DataTypeDateTime64 &>(*normalized_type);
            /// `Variant` timestamps only support microseconds and nanoseconds, so
            /// scales `0..6` map to `6` and scales `7..9` map to `9`.
            UInt32 target_scale = date_time_type.getScale() <= 6 ? 6 : 9;

            Int64 scaled = 0;
            if (!tryRescaleVariantTemporalValue(decimal.getValue().value, date_time_type.getScale(), target_scale, scaled))
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
            if (!tryConvertIntegralFieldValue(field, converted))
                return false;

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(converted, 0, 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `Time` value as `Parquet` `VARIANT` time");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::TimeNtzMicros, micros, sink);
            return true;
        }
        case TypeIndex::Time64:
        {
            if (field.getType() != Field::Types::Decimal64)
                return false;

            const auto & decimal = field.safeGet<DecimalField<Time64>>();
            const auto & time_type = assert_cast<const DataTypeTime64 &>(*normalized_type);

            Int64 micros = 0;
            if (!tryRescaleVariantTemporalValue(decimal.getValue().value, time_type.getScale(), 6, micros))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode `Time64` value as `Parquet` `VARIANT` time");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::TimeNtzMicros, micros, sink);
            return true;
        }
        case TypeIndex::Int8:
        {
            Int8 value = 0;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int8, value, sink);
            return true;
        }
        case TypeIndex::Int16:
        {
            Int16 value = 0;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int16, value, sink);
            return true;
        }
        case TypeIndex::Int32:
        {
            Int32 value = 0;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int32, value, sink);
            return true;
        }
        case TypeIndex::UInt8:
        {
            Int16 value = 0;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int16, value, sink);
            return true;
        }
        case TypeIndex::UInt16:
        {
            Int32 value = 0;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int32, value, sink);
            return true;
        }
        case TypeIndex::UInt32:
        {
            Int64 value = 0;
            if (!tryConvertIntegralFieldValue(field, value))
                return false;
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, value, sink);
            return true;
        }
        case TypeIndex::Float32:
        {
            if (field.getType() != Field::Types::Float64)
                return false;

            Float64 value = field.safeGet<Float64>();
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

template <typename Sink>
void encodeVariantScalarField(const Field & field, const DataTypePtr & type_hint, Sink & sink)
{
    if (tryEncodeVariantScalarUsingTypeHint(field, type_hint, sink))
        return;

    switch (field.getType())
    {
        case Field::Types::Null:
            sink.writePrimitiveHeader(VariantPrimitiveType::Null);
            return;
        case Field::Types::Bool:
            sink.writePrimitiveHeader(field.safeGet<bool>() ? VariantPrimitiveType::BooleanTrue : VariantPrimitiveType::BooleanFalse);
            return;
        case Field::Types::Int64:
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, field.safeGet<Int64>(), sink);
            return;
        case Field::Types::UInt64:
        {
            UInt64 source = field.safeGet<UInt64>();
            if (source > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode integer {} as `Parquet` `VARIANT` `INT64`", source);

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, static_cast<Int64>(source), sink);
            return;
        }
        case Field::Types::Int128:
        case Field::Types::UInt128:
        case Field::Types::Int256:
        case Field::Types::UInt256:
        {
            Int64 converted = 0;
            if (!tryConvertIntegralFieldValue(field, converted))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode integer value as `Parquet` `VARIANT` `INT64`");

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, converted, sink);
            return;
        }
        case Field::Types::Float64:
        {
            Float64 source = field.safeGet<Float64>();
            if (!std::isfinite(source))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode non-finite `Parquet` `VARIANT` `DOUBLE`");

            sink.writePrimitiveHeader(VariantPrimitiveType::Double);
            sink.writePOD(source);
            return;
        }
        case Field::Types::String:
            sink.writeString(field.safeGet<String>());
            return;
        case Field::Types::UUID:
        {
            sink.writePrimitiveHeader(VariantPrimitiveType::UUID);
            sink.writeUUID(field.safeGet<UUID>());
            return;
        }
        case Field::Types::Decimal32:
            writeVariantDecimalPrimitive(VariantPrimitiveType::Decimal4, field.safeGet<DecimalField<Decimal32>>(), sink);
            return;
        case Field::Types::Decimal64:
            writeVariantDecimalPrimitive(VariantPrimitiveType::Decimal8, field.safeGet<DecimalField<Decimal64>>(), sink);
            return;
        case Field::Types::Decimal128:
            writeVariantDecimalPrimitive(VariantPrimitiveType::Decimal16, field.safeGet<DecimalField<Decimal128>>(), sink);
            return;
        case Field::Types::IPv4:
        case Field::Types::IPv6:
        {
            WriteBufferFromOwnString wb;
            if (field.getType() == Field::Types::IPv4)
                writeText(field.safeGet<IPv4>(), wb);
            else
                writeText(field.safeGet<IPv6>(), wb);
            String text = wb.str();
            sink.writeString(text);
            return;
        }
        case Field::Types::Decimal256:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot encode `Decimal256` as `Parquet` `VARIANT`");
        default:
            break;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported value while encoding `Parquet` `VARIANT`");
}

template <typename Sink>
void encodeVariantScalarValue(const Field & field, const DataTypePtr & type_hint, Sink & sink)
{
    encodeVariantScalarField(field, type_hint, sink);
}

size_t getVariantScalarEncodedSize(const Field & field, const DataTypePtr & type_hint)
{
    VariantScalarMeasureSink sink;
    encodeVariantScalarValue(field, type_hint, sink);
    return sink.size;
}

void writeVariantScalarToBuffer(const Field & field, const DataTypePtr & type_hint, char *& out)
{
    VariantScalarWriteSink sink(out);
    encodeVariantScalarValue(field, type_hint, sink);
}

size_t getDirectVariantResidualScalarEncodedSize(const DirectVariantResidualEntry & entry)
{
    switch (entry.value_kind)
    {
        case DirectVariantResidualValueKind::Field:
            return getVariantScalarEncodedSize(entry.value, entry.type_hint);
        case DirectVariantResidualValueKind::Null:
        case DirectVariantResidualValueKind::Bool:
            return 1;
        case DirectVariantResidualValueKind::Int64:
        case DirectVariantResidualValueKind::UInt64:
        case DirectVariantResidualValueKind::Float64:
            return 1 + sizeof(UInt64);
        case DirectVariantResidualValueKind::StringView:
            return getVariantEncodedStringSize(entry.string_value);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected direct `Parquet` `VARIANT` residual value kind");
}

void writeDirectVariantResidualEntryToBuffer(const DirectVariantResidualEntry & entry, char *& out)
{
    VariantScalarWriteSink sink(out);
    switch (entry.value_kind)
    {
        case DirectVariantResidualValueKind::Field:
            writeVariantScalarToBuffer(entry.value, entry.type_hint, out);
            return;
        case DirectVariantResidualValueKind::Null:
            sink.writePrimitiveHeader(VariantPrimitiveType::Null);
            return;
        case DirectVariantResidualValueKind::Bool:
            sink.writePrimitiveHeader(entry.bool_value ? VariantPrimitiveType::BooleanTrue : VariantPrimitiveType::BooleanFalse);
            return;
        case DirectVariantResidualValueKind::Int64:
            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, entry.int64_value, sink);
            return;
        case DirectVariantResidualValueKind::UInt64:
        {
            if (entry.uint64_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode integer {} as `Parquet` `VARIANT` `INT64`", entry.uint64_value);

            writeVariantSignedIntegralPrimitive(VariantPrimitiveType::Int64, static_cast<Int64>(entry.uint64_value), sink);
            return;
        }
        case DirectVariantResidualValueKind::Float64:
        {
            if (!std::isfinite(entry.float64_value))
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Cannot encode non-finite `Parquet` `VARIANT` `DOUBLE`");

            sink.writePrimitiveHeader(VariantPrimitiveType::Double);
            sink.writePOD(entry.float64_value);
            return;
        }
        case DirectVariantResidualValueKind::StringView:
            sink.writeString(entry.string_value);
            return;
    }
}

struct MeasuredVariantObjectChild
{
    UInt32 field_id = 0;
    const Field * child = nullptr;
    DataTypePtr type_hint;
    String path;
    size_t child_size = 0;
};

struct MeasuredVariantObjectEncoding
{
    bool omitted = false;
    UInt8 field_id_size = 0;
    UInt8 field_offset_size = 0;
    bool is_large = false;
    size_t total_children_size = 0;
    size_t total_size = 0;
    std::vector<MeasuredVariantObjectChild> children;
};

struct MeasuredVariantArrayEncoding
{
    UInt8 field_offset_size = 0;
    bool is_large = false;
    size_t total_children_size = 0;
    size_t total_size = 0;
    std::vector<size_t> child_sizes;
};

size_t measureVariantValueEncodedSize(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    const std::unordered_set<String> * excluded_fields = nullptr);

MeasuredVariantObjectEncoding measureVariantObjectEncoding(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    const std::unordered_set<String> * excluded_fields = nullptr)
{
    chassert(value.getType() == Field::Types::Object);
    const auto & object = value.safeGet<Object>();

    MeasuredVariantObjectEncoding encoding;
    encoding.children.reserve(object.size());

    UInt32 highest_field_id = 0;
    for (const auto & [key, child_value] : object)
    {
        if (excluded_fields && excluded_fields->contains(key))
            continue;

        auto it = dictionary.find(key);
        if (it == dictionary.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing `Parquet` `VARIANT` dictionary entry for key {}", key);

        String child_path = appendVariantJSONPath(current_path, key);
        DataTypePtr child_type_hint = getObjectChildTypeHint(type_hint, object_type, child_path, key);
        size_t child_size = measureVariantValueEncodedSize(child_value, child_type_hint, object_type, child_path, dictionary);
        highest_field_id = std::max(highest_field_id, it->second);
        addVariantEncodingSizeOrThrow(encoding.total_children_size, child_size, "`object` children");
        encoding.children.emplace_back(MeasuredVariantObjectChild
        {
            .field_id = it->second,
            .child = &child_value,
            .type_hint = std::move(child_type_hint),
            .path = std::move(child_path),
            .child_size = child_size,
        });
    }

    if (!object.empty() && encoding.children.empty())
    {
        encoding.omitted = true;
        return encoding;
    }

    finishVariantObjectEncodingHeader(
        encoding.children.size(),
        highest_field_id,
        encoding.total_children_size,
        encoding.field_id_size,
        encoding.field_offset_size,
        encoding.is_large,
        encoding.total_size);
    return encoding;
}

MeasuredVariantArrayEncoding measureVariantArrayEncoding(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary)
{
    chassert(value.getType() == Field::Types::Array);
    const auto & array = value.safeGet<Array>();

    MeasuredVariantArrayEncoding encoding;
    encoding.child_sizes.reserve(array.size());
    for (size_t i = 0; i < array.size(); ++i)
    {
        size_t child_size = measureVariantValueEncodedSize(array[i], getArrayChildTypeHint(type_hint, i), object_type, current_path, dictionary);
        addVariantEncodingSizeOrThrow(encoding.total_children_size, child_size, "`array` children");
        encoding.child_sizes.emplace_back(child_size);
    }

    encoding.field_offset_size = variantByteLength(encoding.total_children_size);
    if (encoding.field_offset_size > 4)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Cannot encode `Parquet` `VARIANT` array header requiring offset size {}; maximum supported header width is 4 bytes",
            static_cast<UInt32>(encoding.field_offset_size));
    }

    if (encoding.child_sizes.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Cannot encode `Parquet` `VARIANT` array with more than {} elements", std::numeric_limits<UInt32>::max());

    encoding.is_large = encoding.child_sizes.size() > std::numeric_limits<UInt8>::max();
    size_t total_size = 1;
    addVariantEncodingSizeOrThrow(total_size, encoding.is_large ? sizeof(UInt32) : sizeof(UInt8), "`array` header");
    addVariantEncodingSizeOrThrow(
        total_size,
        multiplyVariantEncodingSizeOrThrow(
            addVariantEncodingSizesOrThrow(encoding.child_sizes.size(), size_t{1}, "`array` offset table size"),
            encoding.field_offset_size,
            "`array` field offset table"),
        "`array` total size");
    addVariantEncodingSizeOrThrow(total_size, encoding.total_children_size, "`array` total size");
    encoding.total_size = total_size;
    return encoding;
}

size_t measureVariantValueEncodedSize(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    const std::unordered_set<String> * excluded_fields)
{
    if (value.getType() != Field::Types::Object && value.getType() != Field::Types::Array)
        return getVariantScalarEncodedSize(value, type_hint);

    if (value.getType() == Field::Types::Object)
        return measureVariantObjectEncoding(value, type_hint, object_type, current_path, dictionary, excluded_fields).total_size;

    return measureVariantArrayEncoding(value, type_hint, object_type, current_path, dictionary).total_size;
}

void writeVariantEncodedValueToBuffer(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    char *& out);

void writeVariantObjectToBuffer(
    const DataTypeObject * object_type,
    const std::unordered_map<String, UInt32> & dictionary,
    const MeasuredVariantObjectEncoding & encoding,
    char *& out)
{
    chassert(!encoding.omitted);

    writeVariantObjectHeaderAndChildren(
        encoding.children,
        encoding.field_id_size,
        encoding.field_offset_size,
        encoding.is_large,
        out,
        [&](const MeasuredVariantObjectChild & child)
        {
            writeVariantEncodedValueToBuffer(*child.child, child.type_hint, object_type, child.path, dictionary, out);
        });
}

void writeVariantArrayToBuffer(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    const MeasuredVariantArrayEncoding & encoding,
    char *& out)
{
    chassert(value.getType() == Field::Types::Array);
    const auto & array = value.safeGet<Array>();

    UInt8 header = static_cast<UInt8>(VariantBasicType::Array);
    header |= static_cast<UInt8>(encoding.field_offset_size - 1) << VARIANT_VALUE_HEADER_SHIFT;
    header |= static_cast<UInt8>(encoding.is_large) << (VARIANT_VALUE_HEADER_SHIFT + VARIANT_ARRAY_IS_LARGE_SHIFT);
    *out = static_cast<char>(header);
    ++out;

    if (encoding.is_large)
        writeVariantPODToBuffer(static_cast<UInt32>(encoding.child_sizes.size()), out);
    else
    {
        *out = static_cast<char>(static_cast<UInt8>(encoding.child_sizes.size()));
        ++out;
    }

    UInt64 offset = 0;
    for (size_t child_size : encoding.child_sizes)
    {
        writeVariantLittleEndianToBuffer(offset, encoding.field_offset_size, out);
        addVariantEncodingUInt64OrThrow(offset, static_cast<UInt64>(child_size), "`array` child offsets");
    }
    writeVariantLittleEndianToBuffer(offset, encoding.field_offset_size, out);

    for (size_t i = 0; i < array.size(); ++i)
        writeVariantEncodedValueToBuffer(array[i], getArrayChildTypeHint(type_hint, i), object_type, current_path, dictionary, out);
}

void writeVariantEncodedValueToBuffer(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    char *& out)
{
    if (value.getType() != Field::Types::Object && value.getType() != Field::Types::Array)
    {
        writeVariantScalarToBuffer(value, type_hint, out);
        return;
    }

    if (value.getType() == Field::Types::Object)
    {
        auto encoding = measureVariantObjectEncoding(value, type_hint, object_type, current_path, dictionary);
        writeVariantObjectToBuffer(object_type, dictionary, encoding, out);
        return;
    }

    auto encoding = measureVariantArrayEncoding(value, type_hint, object_type, current_path, dictionary);
    writeVariantArrayToBuffer(value, type_hint, object_type, current_path, dictionary, encoding, out);
}

void encodeVariantObject(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    std::optional<String> & out,
    const std::unordered_set<String> * excluded_fields = nullptr)
{
    chassert(value.getType() == Field::Types::Object);
    auto encoding = measureVariantObjectEncoding(value, type_hint, object_type, current_path, dictionary, excluded_fields);
    if (encoding.omitted)
    {
        out = std::nullopt;
        return;
    }

    out = String(encoding.total_size, '\0');
    char * out_pos = out->data();
    writeVariantObjectToBuffer(object_type, dictionary, encoding, out_pos);
    chassert(out_pos == out->data() + out->size());
}

void encodeVariantValue(
    const Field & value,
    const DataTypePtr & type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const std::unordered_map<String, UInt32> & dictionary,
    String & out)
{
    size_t total_size = measureVariantValueEncodedSize(value, type_hint, object_type, current_path, dictionary);
    out.resize(total_size);
    char * out_pos = out.data();
    writeVariantEncodedValueToBuffer(value, type_hint, object_type, current_path, dictionary, out_pos);
    chassert(out_pos == out.data() + out.size());
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

bool transformVariantElement(
    const Field & value,
    const DataTypePtr & value_type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const DataTypePtr & shredded_type,
    const VariantEncodingContext & context,
    VariantTransformScratch & scratch,
    VariantTransformResult & out);

bool transformVariantElement(
    const Field & value,
    const DataTypePtr & value_type_hint,
    const DataTypeObject * object_type,
    std::string_view current_path,
    const DataTypePtr & shredded_type,
    const VariantEncodingContext & context,
    VariantTransformScratch & scratch,
    VariantTransformResult & out)
{
    DataTypePtr normalized_shredded_type = unwrapVariantTypeHint(shredded_type);
    auto build_wrapper_field = [&](const Field & nested_value, const DataTypePtr & nested_value_type_hint, const DataTypePtr & wrapper_type, std::string_view nested_path, Field & wrapped_field)
    {
        const IDataType & wrapper_data_type = typeid_cast<const DataTypeNullable *>(wrapper_type.get())
            ? *assert_cast<const DataTypeNullable &>(*wrapper_type).getNestedType()
            : *wrapper_type;
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(wrapper_data_type);
        auto typed_pos = tuple_type.tryGetPositionByName("typed_value").value();
        DataTypePtr typed_nested = tuple_type.getElement(typed_pos);
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(typed_nested.get()))
            typed_nested = nullable->getNestedType();

        VariantTransformResult transformed;
        if (!transformVariantElement(nested_value, nested_value_type_hint, object_type, nested_path, typed_nested, context, scratch, transformed))
            return false;

        auto value_pos = tuple_type.tryGetPositionByName("value").value();
        Tuple result(tuple_type.getElements().size());
        result[value_pos] = transformed.residual_value ? Field(*transformed.residual_value) : Field();
        if (transformed.typed_value.has_value())
            result[typed_pos] = *transformed.typed_value;
        else
        {
            if (!typeid_cast<const DataTypeNullable *>(tuple_type.getElement(typed_pos).get()))
                return false;
            result[typed_pos] = tuple_type.getElement(typed_pos)->getDefault();
        }
        wrapped_field = std::move(result);
        return true;
    };
    auto get_shredded_object_field_names = [&](const DataTypeTuple & tuple_type) -> const std::unordered_set<String> &
    {
        const IDataType * key = &tuple_type;
        auto it = scratch.find(key);
        if (it != scratch.end())
            return it->second;

        std::unordered_set<String> field_names;
        field_names.reserve(tuple_type.getElements().size());
        for (size_t i = 0; i < tuple_type.getElements().size(); ++i)
            field_names.emplace(tuple_type.getNameByPosition(i + 1));

        return scratch.emplace(key, std::move(field_names)).first->second;
    };

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(normalized_shredded_type.get()))
    {
        if (value.getType() != Field::Types::Object)
        {
            String residual_value;
            encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, residual_value);

            out = {
                .residual_value = std::move(residual_value),
                .typed_value = std::nullopt,
            };
            return true;
        }

        const auto & object = value.safeGet<Object>();
        Tuple object_fields;

        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            const String & field_name = tuple_type->getNameByPosition(i + 1);
            auto child_it = object.find(field_name);
            if (child_it == object.end())
            {
                object_fields.emplace_back(Field());
                continue;
            }

            String child_path = appendVariantJSONPath(current_path, field_name);
            Field wrapped_field;
            if (!build_wrapper_field(
                    child_it->second,
                    getObjectChildTypeHint(value_type_hint, object_type, child_path, field_name),
                    tuple_type->getElement(i),
                    child_path,
                    wrapped_field))
            {
                return false;
            }

            object_fields.emplace_back(std::move(wrapped_field));
        }

        std::optional<String> residual_value;
        if (object.empty())
        {
            String encoded_object;
            encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, encoded_object);
            residual_value = std::move(encoded_object);
        }
        else
        {
            encodeVariantObject(value, value_type_hint, object_type, current_path, context.dictionary, residual_value, &get_shredded_object_field_names(*tuple_type));
        }

        out = {
            .residual_value = std::move(residual_value),
            .typed_value = Field(object_fields),
        };
        return true;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(normalized_shredded_type.get()))
    {
        if (value.getType() != Field::Types::Array)
        {
            String residual_value;
            encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, residual_value);

            out = {
                .residual_value = std::move(residual_value),
                .typed_value = std::nullopt,
            };
            return true;
        }

        const auto & array = value.safeGet<Array>();
        Array values;
        values.reserve(array.size());

        for (size_t i = 0; i < array.size(); ++i)
        {
            Field wrapped_field;
            if (!build_wrapper_field(
                    array[i],
                    getArrayChildTypeHint(value_type_hint, i),
                    array_type->getNestedType(),
                    current_path,
                    wrapped_field))
            {
                return false;
            }

            values.emplace_back(std::move(wrapped_field));
        }

        out = {
            .residual_value = std::nullopt,
            .typed_value = Field(values),
        };
        return true;
    }

    if (auto typed_scalar = tryConvertVariantScalarToShreddedField(value, normalized_shredded_type))
    {
        out = {
            .residual_value = std::nullopt,
            .typed_value = std::move(typed_scalar),
        };
        return true;
    }

    String residual_value;
    encodeVariantValue(value, value_type_hint, object_type, current_path, context.dictionary, residual_value);

    out = {
        .residual_value = std::move(residual_value),
        .typed_value = std::nullopt,
    };
    return true;
}

void buildVariantRowForWrite(
    const IColumn & full_column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    size_t row,
    VariantBuildStats stats,
    Field & row_value,
    DataTypePtr & row_type_hint)
{
    const auto * object_type = typeid_cast<const DataTypeObject *>(type.get());

    if (object_type)
    {
        if (object_type->getSchemaFormat() != DataTypeObject::SchemaFormat::JSON)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `JSON` `Object` columns can be written as `Parquet` `VARIANT`");

        const auto * object_column = typeid_cast<const ColumnObject *>(&full_column);
        if (!object_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected `ColumnObject` while preparing `Parquet` `VARIANT` write columns");

        Field nested_row;
        if (!tryBuildNestedObjectFieldFromColumnObject(*object_column, row, format_settings, 1, nested_row))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot convert `Object` row {} to nested JSON shape for `Parquet` `VARIANT` writing", row);

        if (!buildVariantField(nested_row, object_type->getPtr(), object_type, std::string_view{}, format_settings, 1, stats, row_value))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot prepare `Object` row {} for `Parquet` `VARIANT` writing", row);

        row_type_hint = type;
        return;
    }

    if (typeid_cast<const DataTypeDynamic *>(type.get()))
    {
        if (!buildVariantFieldFromColumn(full_column, type, row, format_settings, 1, stats, row_value, &row_type_hint))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot prepare `Parquet` `VARIANT` write value for row {}", row);
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type {} while preparing `Parquet` `VARIANT` write columns", type->getName());
}

void collectVariantRowsForWrite(
    const IColumn & full_column,
    const DataTypePtr & type,
    const FormatSettings & format_settings,
    std::unordered_set<String> * out_unique_keys,
    VariantWriteAnalysisNode * out_analysis,
    std::vector<Field> * out_rows,
    std::vector<DataTypePtr> * out_row_type_hints)
{
    const size_t num_rows = full_column.size();

    if (out_rows)
        out_rows->reserve(num_rows);
    if (out_row_type_hints)
        out_row_type_hints->reserve(num_rows);

    for (size_t row = 0; row < num_rows; ++row)
    {
        Field row_value;
        DataTypePtr row_type_hint;
        buildVariantRowForWrite(
            full_column,
            type,
            format_settings,
            row,
            VariantBuildStats
            {
                .keys = out_unique_keys,
                .analysis = out_analysis,
            },
            row_value,
            row_type_hint);

        if (out_rows)
            out_rows->emplace_back(std::move(row_value));
        if (out_row_type_hints)
            out_row_type_hints->emplace_back(std::move(row_type_hint));
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
    }

    std::vector<Field> rows;
    std::vector<DataTypePtr> row_type_hints;
    std::unordered_set<String> unique_keys;
    VariantWriteAnalysisNode analysis;
    collectVariantRowsForWrite(
        full_column,
        type,
        format_settings,
        &unique_keys,
        need_inference ? &analysis : nullptr,
        &rows,
        &row_type_hints);

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
    VariantTransformScratch transform_scratch;

    auto encode_row = [&](Field row_value, DataTypePtr row_type_hint)
    {
        row_type_hint = row_type_hint ? row_type_hint : type;

        metadata_string_column.insertData(shared_context.metadata.data(), shared_context.metadata.size());

        if (!shredded_type)
        {
            /// `Dynamic` carries a per-row type hint, so untyped residual payloads can still
            /// preserve explicit temporal tags instead of flattening them to strings.
            /// Keep the older normalization only for `Object`/`JSON`, where untyped residuals
            /// still follow the wider JSON-oriented mapping policy.
            if (object_type)
                normalizeVariantFieldForUntypedResidual(row_value, row_type_hint, object_type, std::string_view{}, format_settings, 1);

            String encoded_value;
            encodeVariantValue(row_value, row_type_hint, object_type, std::string_view{}, shared_context.dictionary, encoded_value);
            assert_cast<ColumnString &>(*value).insertData(encoded_value.data(), encoded_value.size());
            return;
        }

        VariantTransformResult transformed;
        if (!transformVariantElement(row_value, row_type_hint, object_type, std::string_view{}, shredded_type, shared_context, transform_scratch, transformed))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported shredded value while encoding `Parquet` `VARIANT`");

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

    for (size_t row = 0; row < rows.size(); ++row)
        encode_row(std::move(rows[row]), std::move(row_type_hints[row]));

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

    collectVariantRowsForWrite(column, type, format_settings, nullptr, &out_analysis.analysis, nullptr, nullptr);
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
