#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesCache.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/checkStackSize.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <vector>

#include <fmt/format.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int TOO_DEEP_RECURSION;
}

namespace DB::Parquet
{

namespace
{

/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
enum class PrimitiveType : UInt8
{
    Null = 0,
    True = 1,
    False = 2,
    Int8 = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Double = 7,
    Decimal4 = 8,
    Decimal8 = 9,
    Decimal16 = 10,
    Date = 11,
    TimestampTZ = 12,
    TimestampNTZ = 13,
    Float = 14,
    Binary = 15,
    String = 16,
    TimeNTZ = 17,
    TimestampNanosTZ = 18,
    TimestampNanosNTZ = 19,
    UUID = 20
};

enum class BasicType : UInt8
{
    Primitive = 0,
    ShortString = 1,
    Object = 2,
    Array = 3
};

void checkRange(std::string_view data, size_t pos, size_t size)
{
    if (pos > data.size() || size > data.size() - pos)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Malformed Parquet variant: {} bytes are needed at offset {}, but the blob is {} bytes",
            size, pos, data.size());
}

template <typename T>
T readFixed(std::string_view data, size_t pos)
{
    checkRange(data, pos, sizeof(T));
    return unalignedLoadLittleEndian<T>(data.data() + pos);
}

UInt8 readByte(std::string_view data, size_t pos)
{
    checkRange(data, pos, 1);
    return UInt8(data[pos]);
}

std::string_view readSlice(std::string_view data, size_t pos, size_t size)
{
    checkRange(data, pos, size);
    return data.substr(pos, size);
}

UInt32 readUnsigned(std::string_view data, size_t pos, UInt8 size)
{
    checkRange(data, pos, size);
    UInt32 res = 0;
    for (UInt8 i = 0; i < size; ++i)
        res |= UInt32(UInt8(data[pos + i])) << (8 * i);
    return res;
}

struct Metadata
{
    std::string_view blob;
    size_t offsets_pos = 0;
    size_t names_pos = 0;
    UInt32 count = 0;
    UInt8 offset_size = 0;

    std::string_view getName(UInt32 id) const
    {
        if (id >= count)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Malformed Parquet variant: object field refers to dictionary entry {}, but the metadata "
                "dictionary has {} entries", id, count);
        const UInt32 begin = readUnsigned(blob, offsets_pos + size_t(id) * offset_size, offset_size);
        const UInt32 end = readUnsigned(blob, offsets_pos + (size_t(id) + 1) * offset_size, offset_size);
        if (end < begin)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Malformed Parquet variant: metadata dictionary entry {} ends at {}, before its start {}",
                id, end, begin);
        return readSlice(blob, names_pos + begin, end - begin);
    }
};

Metadata parseMetadata(std::string_view blob)
{
    const UInt8 header = readByte(blob, 0);
    const UInt8 version = header & 0x0F;
    if (version != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Parquet variant metadata version {}", UInt16(version));

    Metadata res;
    res.blob = blob;
    res.offset_size = ((header >> 6) & 0x03) + 1;
    res.count = readUnsigned(blob, 1, res.offset_size);
    res.offsets_pos = 1 + res.offset_size;
    res.names_pos = res.offsets_pos + (size_t(res.count) + 1) * res.offset_size;
    checkRange(blob, res.offsets_pos, res.names_pos - res.offsets_pos);
    return res;
}

struct DecodeContext
{
    const Metadata & metadata;
    size_t max_depth;
};

void checkDepth(const DecodeContext & context, size_t depth)
{
    checkStackSize();
    if (context.max_depth != 0 && depth > context.max_depth)
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Parquet variant value is nested deeper than the limit ({}). It can be raised with the "
            "setting 'max_parser_depth', but a very deeply nested value is rarely intentional",
            context.max_depth);
}

struct ObjectLayout
{
    UInt32 num_elements = 0;
    UInt8 id_size = 0;
    UInt8 offset_size = 0;
    size_t ids_pos = 0;
    size_t offsets_pos = 0;
    size_t values_pos = 0;

    UInt32 fieldId(std::string_view data, UInt32 i) const
    {
        return readUnsigned(data, ids_pos + size_t(i) * id_size, id_size);
    }

    size_t valuePos(std::string_view data, UInt32 i) const
    {
        return values_pos + readUnsigned(data, offsets_pos + size_t(i) * offset_size, offset_size);
    }
};

ObjectLayout parseObjectLayout(std::string_view data, size_t pos, UInt8 value_header)
{
    const bool is_large = (value_header >> 4) & 0x01;
    ObjectLayout res;
    res.id_size = ((value_header >> 2) & 0x03) + 1;
    res.offset_size = (value_header & 0x03) + 1;
    res.num_elements = readUnsigned(data, pos, is_large ? 4 : 1);
    res.ids_pos = pos + (is_large ? 4 : 1);
    res.offsets_pos = res.ids_pos + size_t(res.num_elements) * res.id_size;
    res.values_pos = res.offsets_pos + (size_t(res.num_elements) + 1) * res.offset_size;
    checkRange(data, res.ids_pos, res.values_pos - res.ids_pos);
    return res;
}

struct ArrayLayout
{
    UInt32 num_elements = 0;
    UInt8 offset_size = 0;
    size_t offsets_pos = 0;
    size_t values_pos = 0;

    size_t elementPos(std::string_view data, UInt32 i) const
    {
        return values_pos + readUnsigned(data, offsets_pos + size_t(i) * offset_size, offset_size);
    }
};

ArrayLayout parseArrayLayout(std::string_view data, size_t pos, UInt8 value_header)
{
    const bool is_large = (value_header >> 2) & 0x01;
    ArrayLayout res;
    res.offset_size = (value_header & 0x03) + 1;
    res.num_elements = readUnsigned(data, pos, is_large ? 4 : 1);
    res.offsets_pos = pos + (is_large ? 4 : 1);
    res.values_pos = res.offsets_pos + (size_t(res.num_elements) + 1) * res.offset_size;
    checkRange(data, res.offsets_pos, res.values_pos - res.offsets_pos);
    return res;
}

bool isObject(std::string_view data, size_t pos)
{
    return BasicType(readByte(data, pos) & 0x03) == BasicType::Object;
}

bool isArrayOfObjects(std::string_view data, size_t pos, UInt8 value_header)
{
    const ArrayLayout layout = parseArrayLayout(data, pos, value_header);
    if (layout.num_elements == 0)
        return false;
    for (UInt32 i = 0; i < layout.num_elements; ++i)
    {
        if (!isObject(data, layout.elementPos(data, i)))
            return false;
    }
    return true;
}

String decimalTypeName(std::string_view data, size_t pos, UInt8 precision)
{
    const UInt8 scale = readByte(data, pos + 1);
    if (scale > precision)
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Malformed Parquet variant: decimal with precision {} has invalid scale {}", UInt16(precision), UInt16(scale));
    return fmt::format("Decimal({}, {})", UInt16(precision), UInt16(scale));
}

/// The name of the type of the value at `pos`, without decoding it. Empty for a variant null.
String getValueTypeName(std::string_view data, size_t pos)
{
    const UInt8 header = readByte(data, pos);
    const UInt8 value_header = header >> 2;

    switch (BasicType(header & 0x03))
    {
        case BasicType::ShortString:
            return "String";
        case BasicType::Object:
            return "JSON";
        case BasicType::Array:
            return isArrayOfObjects(data, pos + 1, value_header) ? "Array(JSON)" : "Array(Dynamic)";
        case BasicType::Primitive:
            break;
    }

    switch (PrimitiveType(value_header))
    {
        case PrimitiveType::Null:
            return {};
        case PrimitiveType::True:
        case PrimitiveType::False:
            return "Bool";
        case PrimitiveType::Int8:
            return "Int8";
        case PrimitiveType::Int16:
            return "Int16";
        case PrimitiveType::Int32:
            return "Int32";
        case PrimitiveType::Int64:
            return "Int64";
        case PrimitiveType::Float:
            return "Float32";
        case PrimitiveType::Double:
            return "Float64";
        case PrimitiveType::Decimal4:
            return decimalTypeName(data, pos, 9);
        case PrimitiveType::Decimal8:
            return decimalTypeName(data, pos, 18);
        case PrimitiveType::Decimal16:
            return decimalTypeName(data, pos, 38);
        case PrimitiveType::Date:
            return "Date32";
        case PrimitiveType::TimestampTZ:
            return "DateTime64(6, 'UTC')";
        case PrimitiveType::TimestampNTZ:
            return "DateTime64(6)";
        case PrimitiveType::TimestampNanosTZ:
            return "DateTime64(9, 'UTC')";
        case PrimitiveType::TimestampNanosNTZ:
            return "DateTime64(9)";
        case PrimitiveType::TimeNTZ:
            return "Time64(6)";
        case PrimitiveType::UUID:
            return "UUID";
        case PrimitiveType::Binary:
        case PrimitiveType::String:
            return "String";
    }

    /// The id is 6 bits of a byte of the blob, so it can be any of 0..63, while the encoding spec
    /// assigns only 0..20.
    throw Exception(
        ErrorCodes::INCORRECT_DATA, "Malformed Parquet variant: unknown primitive type id {}", UInt16(value_header));
}

void decodeValueIntoDynamic(
    std::string_view data, size_t pos, const DecodeContext & context, size_t depth, ColumnDynamic & target);

/// `target` must be a column of the type reported by getValueType for this value.
void decodeValueIntoColumn(
    std::string_view data, size_t pos, const DecodeContext & context, size_t depth, IColumn & target);

void decodePrimitiveIntoColumn(std::string_view data, size_t pos, PrimitiveType type_id, IColumn & target)
{
    switch (type_id)
    {
        case PrimitiveType::True:
            assert_cast<ColumnUInt8 &>(target).insertValue(1);
            return;
        case PrimitiveType::False:
            assert_cast<ColumnUInt8 &>(target).insertValue(0);
            return;
        case PrimitiveType::Int8:
            assert_cast<ColumnInt8 &>(target).insertValue(readFixed<Int8>(data, pos));
            return;
        case PrimitiveType::Int16:
            assert_cast<ColumnInt16 &>(target).insertValue(readFixed<Int16>(data, pos));
            return;
        case PrimitiveType::Int32:
            assert_cast<ColumnInt32 &>(target).insertValue(readFixed<Int32>(data, pos));
            return;
        case PrimitiveType::Int64:
            assert_cast<ColumnInt64 &>(target).insertValue(readFixed<Int64>(data, pos));
            return;
        case PrimitiveType::Float:
            assert_cast<ColumnFloat32 &>(target).insertValue(readFixed<Float32>(data, pos));
            return;
        case PrimitiveType::Double:
            assert_cast<ColumnFloat64 &>(target).insertValue(readFixed<Float64>(data, pos));
            return;
        case PrimitiveType::Decimal4:
            assert_cast<ColumnDecimal<Decimal32> &>(target).insertValue(Decimal32(readFixed<Int32>(data, pos + 1)));
            return;
        case PrimitiveType::Decimal8:
            assert_cast<ColumnDecimal<Decimal64> &>(target).insertValue(Decimal64(readFixed<Int64>(data, pos + 1)));
            return;
        case PrimitiveType::Decimal16:
            assert_cast<ColumnDecimal<Decimal128> &>(target).insertValue(Decimal128(readFixed<Int128>(data, pos + 1)));
            return;
        case PrimitiveType::Date:
            assert_cast<ColumnDate32 &>(target).insertValue(readFixed<Int32>(data, pos));
            return;
        case PrimitiveType::TimestampTZ:
        case PrimitiveType::TimestampNTZ:
        case PrimitiveType::TimestampNanosTZ:
        case PrimitiveType::TimestampNanosNTZ:
            assert_cast<ColumnDecimal<DateTime64> &>(target).insertValue(DateTime64(readFixed<Int64>(data, pos)));
            return;
        case PrimitiveType::TimeNTZ:
            assert_cast<ColumnDecimal<Time64> &>(target).insertValue(Time64(readFixed<Int64>(data, pos)));
            return;
        case PrimitiveType::UUID:
        {
            const std::string_view bytes = readSlice(data, pos, sizeof(UUID));
            UUID uuid;
            memcpy(&uuid, bytes.data(), sizeof(UUID));
            auto * halves = reinterpret_cast<UInt8 *>(&uuid);
            if constexpr (std::endian::native == std::endian::little)
            {
                std::reverse(halves, halves + 8);
                std::reverse(halves + 8, halves + 16);
            }
            else
            {
                std::swap_ranges(halves, halves + 8, halves + 8);
            }
            assert_cast<ColumnUUID &>(target).insertValue(uuid);
            return;
        }
        case PrimitiveType::Binary:
        case PrimitiveType::String:
        {
            const UInt32 length = readFixed<UInt32>(data, pos);
            const std::string_view value = readSlice(data, pos + 4, length);
            assert_cast<ColumnString &>(target).insertData(value.data(), value.size());
            return;
        }
        case PrimitiveType::Null:
            break;
    }

    throw Exception(
        ErrorCodes::INCORRECT_DATA, "Malformed Parquet variant: unknown primitive type id {}", UInt16(type_id));
}

struct SharedDataValue
{
    String path;
    size_t pos;
    size_t depth;
};

/// Nested objects are flattened into dot-separated paths, the same way the `JSON` type stores them.
void insertObjectPaths(
    std::string_view data,
    size_t pos,
    UInt8 value_header,
    const DecodeContext & context,
    size_t depth,
    const String & prefix,
    bool is_root,
    ColumnObject & column,
    size_t prev_size,
    std::vector<SharedDataValue> & shared_data_values)
{
    const ObjectLayout layout = parseObjectLayout(data, pos, value_header);

    for (UInt32 i = 0; i < layout.num_elements; ++i)
    {
        const std::string_view name = context.metadata.getName(layout.fieldId(data, i));
        String path = is_root ? String(name) : prefix + "." + String(name);
        const size_t value_pos = layout.valuePos(data, i);
        const UInt8 header = readByte(data, value_pos);

        if (BasicType(header & 0x03) == BasicType::Object)
        {
            checkDepth(context, depth + 1);
            insertObjectPaths(data, value_pos + 1, header >> 2, context, depth + 1, path, false, column, prev_size, shared_data_values);
            continue;
        }

        /// Like in the `JSON` type, a null is equivalent to the absence of the path.
        if (header == UInt8(PrimitiveType::Null) << 2)
            continue;

        ColumnDynamic * dynamic_column = nullptr;
        auto & dynamic_paths = column.getDynamicPathsPtrs();
        if (auto it = dynamic_paths.find(path); it != dynamic_paths.end())
        {
            dynamic_column = it->second;
            if (dynamic_column->size() > prev_size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Parquet variant object has the path '{}' more than once after flattening nested objects", path);
        }
        else
        {
            dynamic_column = column.tryToAddNewDynamicPath(path);
        }

        if (!dynamic_column)
        {
            shared_data_values.push_back({std::move(path), value_pos, depth + 1});
            continue;
        }

        decodeValueIntoDynamic(data, value_pos, context, depth + 1, *dynamic_column);
    }
}

void decodeObjectIntoJSON(
    std::string_view data, size_t pos, UInt8 value_header, const DecodeContext & context, size_t depth, ColumnObject & column)
{
    const size_t prev_size = column.size();

    std::vector<SharedDataValue> shared_data_values;
    insertObjectPaths(data, pos, value_header, context, depth, "", true, column, prev_size, shared_data_values);

    /// Paths in shared data must be sorted.
    std::sort(
        shared_data_values.begin(), shared_data_values.end(), [](const auto & left, const auto & right) { return left.path < right.path; });

    auto [shared_data_paths, shared_data_values_column] = column.getSharedDataPathsAndValues();
    MutableColumnPtr tmp_dynamic_column;
    for (size_t i = 0; i < shared_data_values.size(); ++i)
    {
        const SharedDataValue & value = shared_data_values[i];
        if (i != 0 && value.path == shared_data_values[i - 1].path)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Parquet variant object has the path '{}' more than once after flattening nested objects", value.path);

        if (!tmp_dynamic_column)
            tmp_dynamic_column = ColumnDynamic::create(column.getMaxDynamicTypes());
        auto & tmp_dynamic = assert_cast<ColumnDynamic &>(*tmp_dynamic_column);
        decodeValueIntoDynamic(data, value.pos, context, value.depth, tmp_dynamic);
        ColumnObject::serializePathAndValueIntoSharedData(
            shared_data_paths, shared_data_values_column, value.path, tmp_dynamic, tmp_dynamic.size() - 1);
    }
    column.getSharedDataOffsets().push_back(shared_data_paths->size());

    for (auto & [_, dynamic_column] : column.getDynamicPathsPtrs())
    {
        if (dynamic_column->size() == prev_size)
            dynamic_column->insertDefault();
    }
}

void decodeArrayIntoColumn(
    std::string_view data, size_t pos, UInt8 value_header, const DecodeContext & context, size_t depth, IColumn & target)
{
    const ArrayLayout layout = parseArrayLayout(data, pos, value_header);

    auto & array_column = assert_cast<ColumnArray &>(target);
    auto & elements = array_column.getData();

    if (auto * objects = typeid_cast<ColumnObject *>(&elements))
    {
        for (UInt32 i = 0; i < layout.num_elements; ++i)
        {
            const size_t element_pos = layout.elementPos(data, i);
            checkDepth(context, depth + 1);
            decodeObjectIntoJSON(data, element_pos + 1, readByte(data, element_pos) >> 2, context, depth + 1, *objects);
        }
    }
    else
    {
        auto & dynamic_elements = assert_cast<ColumnDynamic &>(elements);
        for (UInt32 i = 0; i < layout.num_elements; ++i)
            decodeValueIntoDynamic(data, layout.elementPos(data, i), context, depth + 1, dynamic_elements);
    }

    array_column.getOffsets().push_back(elements.size());
}

void decodeValueIntoColumn(
    std::string_view data, size_t pos, const DecodeContext & context, size_t depth, IColumn & target)
{
    const UInt8 header = readByte(data, pos);
    const UInt8 value_header = header >> 2;

    switch (BasicType(header & 0x03))
    {
        case BasicType::Primitive:
            decodePrimitiveIntoColumn(data, pos + 1, PrimitiveType(value_header), target);
            return;
        case BasicType::ShortString:
        {
            const std::string_view value = readSlice(data, pos + 1, value_header);
            assert_cast<ColumnString &>(target).insertData(value.data(), value.size());
            return;
        }
        case BasicType::Object:
            decodeObjectIntoJSON(data, pos + 1, value_header, context, depth, assert_cast<ColumnObject &>(target));
            return;
        case BasicType::Array:
            decodeArrayIntoColumn(data, pos + 1, value_header, context, depth, target);
            return;
    }
}

void decodeValueIntoDynamic(
    std::string_view data, size_t pos, const DecodeContext & context, size_t depth, ColumnDynamic & target)
{
    checkDepth(context, depth);

    const String type_name = getValueTypeName(data, pos);
    if (type_name.empty())
    {
        target.insertDefault();
        return;
    }

    /// extendVariantColumn mutates the variant column in place, so this reference survives
    /// addNewVariant below.
    auto & variant_column = target.getVariantColumn();

    if (target.getVariantInfo().variant_name_to_discriminator.contains(type_name)
        || target.addNewVariant(getDataTypesCache().getType(type_name), type_name))
    {
        const ColumnVariant::Discriminator discriminator
            = target.getVariantInfo().variant_name_to_discriminator.at(type_name);
        auto & variant = variant_column.getVariantByGlobalDiscriminator(discriminator);
        decodeValueIntoColumn(data, pos, context, depth, variant);
        variant_column.getOffsets().push_back(variant.size() - 1);
        variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(discriminator));
        return;
    }

    /// The Dynamic is out of variant slots, so the value goes into the shared variant, which needs
    /// it as a standalone column.
    const DataTypePtr type = getDataTypesCache().getType(type_name);
    auto single_value_column = type->createColumn();
    decodeValueIntoColumn(data, pos, context, depth, *single_value_column);
    target.insertValueIntoSharedVariant(*single_value_column, type, type_name, 0);
}

const ColumnString & unwrapLeaf(const IColumn & column, const NullMap *& out_null_map)
{
    const IColumn * inner = &column;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        out_null_map = &nullable->getNullMapData();
        inner = &nullable->getNestedColumn();
    }
    return assert_cast<const ColumnString &>(*inner);
}

}

void decodeVariantColumn(
    const IColumn & metadata,
    const IColumn & value,
    ColumnDynamic & output,
    size_t num_rows,
    size_t max_parser_depth)
{
    const NullMap * metadata_nulls = nullptr;
    const ColumnString & metadata_strings = unwrapLeaf(metadata, metadata_nulls);

    const NullMap * value_nulls = nullptr;
    const ColumnString & value_strings = unwrapLeaf(value, value_nulls);

    for (size_t row = 0; row < num_rows; ++row)
    {
        if ((metadata_nulls && (*metadata_nulls)[row]) || (value_nulls && (*value_nulls)[row]))
        {
            output.insertDefault();
            continue;
        }

        const std::string_view metadata_blob = metadata_strings.getDataAt(row);
        const std::string_view value_blob = value_strings.getDataAt(row);

        const Metadata parsed_metadata = parseMetadata(metadata_blob);
        const DecodeContext context{.metadata = parsed_metadata, .max_depth = max_parser_depth};
        decodeValueIntoDynamic(value_blob, 0, context, 0, output);
    }
}

}
