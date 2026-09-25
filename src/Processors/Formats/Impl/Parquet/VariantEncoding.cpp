#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/checkStackSize.h>
#include <base/unaligned.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>

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

constexpr size_t NUM_PRIMITIVE_TYPES = 21;

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

/// A type together with its name, so that neither has to be recomputed per value: going through
/// DataTypeFactory or IDataType::getName for every decoded value dominates the decoding cost.
struct TypeEntry
{
    DataTypePtr type;
    String name;
};

struct TypeTables
{
    std::array<TypeEntry, NUM_PRIMITIVE_TYPES> primitive;
    /// Decimal scale comes from the value itself, so these are indexed by scale.
    std::array<TypeEntry, 10> decimal4;
    std::array<TypeEntry, 19> decimal8;
    std::array<TypeEntry, 39> decimal16;
    TypeEntry string;
    TypeEntry object;
    TypeEntry array;

    static TypeEntry make(DataTypePtr type)
    {
        String name = type->getName();
        return {std::move(type), std::move(name)};
    }

    TypeTables()
    {
        primitive[UInt8(PrimitiveType::True)] = make(DataTypeFactory::instance().get("Bool"));
        primitive[UInt8(PrimitiveType::False)] = primitive[UInt8(PrimitiveType::True)];
        primitive[UInt8(PrimitiveType::Int8)] = make(std::make_shared<DataTypeInt8>());
        primitive[UInt8(PrimitiveType::Int16)] = make(std::make_shared<DataTypeInt16>());
        primitive[UInt8(PrimitiveType::Int32)] = make(std::make_shared<DataTypeInt32>());
        primitive[UInt8(PrimitiveType::Int64)] = make(std::make_shared<DataTypeInt64>());
        primitive[UInt8(PrimitiveType::Float)] = make(std::make_shared<DataTypeFloat32>());
        primitive[UInt8(PrimitiveType::Double)] = make(std::make_shared<DataTypeFloat64>());
        primitive[UInt8(PrimitiveType::Date)] = make(std::make_shared<DataTypeDate32>());
        primitive[UInt8(PrimitiveType::TimestampTZ)] = make(std::make_shared<DataTypeDateTime64>(6, "UTC"));
        primitive[UInt8(PrimitiveType::TimestampNTZ)] = make(std::make_shared<DataTypeDateTime64>(6));
        primitive[UInt8(PrimitiveType::TimestampNanosTZ)] = make(std::make_shared<DataTypeDateTime64>(9, "UTC"));
        primitive[UInt8(PrimitiveType::TimestampNanosNTZ)] = make(std::make_shared<DataTypeDateTime64>(9));
        primitive[UInt8(PrimitiveType::TimeNTZ)] = make(std::make_shared<DataTypeTime64>(6));
        primitive[UInt8(PrimitiveType::UUID)] = make(std::make_shared<DataTypeUUID>());

        string = make(std::make_shared<DataTypeString>());
        primitive[UInt8(PrimitiveType::Binary)] = string;
        primitive[UInt8(PrimitiveType::String)] = string;

        for (size_t scale = 0; scale < decimal4.size(); ++scale)
            decimal4[scale] = make(std::make_shared<DataTypeDecimal<Decimal32>>(9, scale));
        for (size_t scale = 0; scale < decimal8.size(); ++scale)
            decimal8[scale] = make(std::make_shared<DataTypeDecimal<Decimal64>>(18, scale));
        for (size_t scale = 0; scale < decimal16.size(); ++scale)
            decimal16[scale] = make(std::make_shared<DataTypeDecimal<Decimal128>>(38, scale));

        object = make(std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeDynamic>()));
        array = make(std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>()));
    }
};

const TypeTables & typeTables()
{
    static const TypeTables tables;
    return tables;
}

struct DecodeContext
{
    const Metadata & metadata;
    size_t max_depth;
};

/// The type of the value at `pos`, without decoding it. nullptr for a variant null.
const TypeEntry * getValueType(std::string_view data, size_t pos)
{
    const TypeTables & tables = typeTables();
    const UInt8 header = readByte(data, pos);
    const UInt8 value_header = header >> 2;

    switch (BasicType(header & 0x03))
    {
        case BasicType::ShortString:
            return &tables.string;
        case BasicType::Object:
            return &tables.object;
        case BasicType::Array:
            return &tables.array;
        case BasicType::Primitive:
            break;
    }

    switch (PrimitiveType(value_header))
    {
        case PrimitiveType::Null:
            return nullptr;
        case PrimitiveType::Decimal4:
        {
            const UInt8 scale = readByte(data, pos + 1);
            if (scale >= tables.decimal4.size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet variant decimal4 has invalid scale {}", UInt16(scale));
            return &tables.decimal4[scale];
        }
        case PrimitiveType::Decimal8:
        {
            const UInt8 scale = readByte(data, pos + 1);
            if (scale >= tables.decimal8.size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet variant decimal8 has invalid scale {}", UInt16(scale));
            return &tables.decimal8[scale];
        }
        case PrimitiveType::Decimal16:
        {
            const UInt8 scale = readByte(data, pos + 1);
            if (scale >= tables.decimal16.size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet variant decimal16 has invalid scale {}", UInt16(scale));
            return &tables.decimal16[scale];
        }
        default:
            break;
    }

    /// The id is 6 bits of a byte of the blob, so it can be any of 0..63, while the encoding spec
    /// assigns only 0..20.
    if (value_header >= NUM_PRIMITIVE_TYPES || !tables.primitive[value_header].type)
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Malformed Parquet variant: unknown primitive type id {}", UInt16(value_header));

    return &tables.primitive[value_header];
}

void decodeValueIntoDynamic(std::string_view data, size_t pos, const DecodeContext & context, size_t depth, ColumnDynamic & target);

/// `target` must be a column of the type reported by getValueType for this value.
void decodeValueIntoColumn(std::string_view data, size_t pos, const DecodeContext & context, size_t depth, IColumn & target);

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

void decodeObjectIntoColumn(
    std::string_view data, size_t pos, UInt8 value_header, const DecodeContext & context, size_t depth, IColumn & target)
{
    const bool is_large = (value_header >> 4) & 0x01;
    const UInt8 id_size = ((value_header >> 2) & 0x03) + 1;
    const UInt8 offset_size = (value_header & 0x03) + 1;

    const UInt32 num_elements = readUnsigned(data, pos, is_large ? 4 : 1);
    const size_t ids_pos = pos + (is_large ? 4 : 1);
    const size_t offsets_pos = ids_pos + size_t(num_elements) * id_size;
    const size_t values_pos = offsets_pos + (size_t(num_elements) + 1) * offset_size;
    checkRange(data, ids_pos, values_pos - ids_pos);

    auto & map_column = assert_cast<ColumnMap &>(target);
    auto & key_value = map_column.getNestedData();
    auto & keys = assert_cast<ColumnString &>(key_value.getColumn(0));
    auto & values = assert_cast<ColumnDynamic &>(key_value.getColumn(1));

    for (UInt32 i = 0; i < num_elements; ++i)
    {
        const UInt32 field_id = readUnsigned(data, ids_pos + size_t(i) * id_size, id_size);
        const UInt32 offset = readUnsigned(data, offsets_pos + size_t(i) * offset_size, offset_size);
        const std::string_view name = context.metadata.getName(field_id);
        keys.insertData(name.data(), name.size());
        decodeValueIntoDynamic(data, values_pos + offset, context, depth + 1, values);
    }

    map_column.getNestedColumn().getOffsets().push_back(key_value.size());
}

void decodeArrayIntoColumn(
    std::string_view data, size_t pos, UInt8 value_header, const DecodeContext & context, size_t depth, IColumn & target)
{
    const bool is_large = (value_header >> 2) & 0x01;
    const UInt8 offset_size = (value_header & 0x03) + 1;

    const UInt32 num_elements = readUnsigned(data, pos, is_large ? 4 : 1);
    const size_t offsets_pos = pos + (is_large ? 4 : 1);
    const size_t values_pos = offsets_pos + (size_t(num_elements) + 1) * offset_size;
    checkRange(data, offsets_pos, values_pos - offsets_pos);

    auto & array_column = assert_cast<ColumnArray &>(target);
    auto & elements = assert_cast<ColumnDynamic &>(array_column.getData());

    for (UInt32 i = 0; i < num_elements; ++i)
    {
        const UInt32 offset = readUnsigned(data, offsets_pos + size_t(i) * offset_size, offset_size);
        decodeValueIntoDynamic(data, values_pos + offset, context, depth + 1, elements);
    }

    array_column.getOffsets().push_back(elements.size());
}

void decodeValueIntoColumn(std::string_view data, size_t pos, const DecodeContext & context, size_t depth, IColumn & target)
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
            decodeObjectIntoColumn(data, pos + 1, value_header, context, depth, target);
            return;
        case BasicType::Array:
            decodeArrayIntoColumn(data, pos + 1, value_header, context, depth, target);
            return;
    }
}

void decodeValueIntoDynamic(std::string_view data, size_t pos, const DecodeContext & context, size_t depth, ColumnDynamic & target)
{
    checkStackSize();
    if (context.max_depth != 0 && depth > context.max_depth)
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Parquet variant value is nested deeper than the limit ({}). It can be raised with the "
            "setting 'max_parser_depth', but a very deeply nested value is rarely intentional",
            context.max_depth);

    const TypeEntry * entry = getValueType(data, pos);
    if (!entry)
    {
        target.insertDefault();
        return;
    }

    /// extendVariantColumn mutates the variant column in place, so this reference survives
    /// addNewVariant below.
    auto & variant_column = target.getVariantColumn();

    if (target.getVariantInfo().variant_name_to_discriminator.contains(entry->name)
        || target.addNewVariant(entry->type, entry->name))
    {
        const ColumnVariant::Discriminator discriminator
            = target.getVariantInfo().variant_name_to_discriminator.at(entry->name);
        auto & variant = variant_column.getVariantByGlobalDiscriminator(discriminator);
        decodeValueIntoColumn(data, pos, context, depth, variant);
        variant_column.getOffsets().push_back(variant.size() - 1);
        variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(discriminator));
        return;
    }

    /// The Dynamic is out of variant slots, so the value goes into the shared variant, which needs
    /// it as a standalone column.
    auto single_value_column = entry->type->createColumn();
    decodeValueIntoColumn(data, pos, context, depth, *single_value_column);
    target.insertValueIntoSharedVariant(*single_value_column, entry->type, entry->name, 0);
}

void insertDynamicValueFrom(ColumnDynamic & column, const DataTypePtr & type, const IColumn & src, size_t n)
{
    const String type_name = type->getName();

    auto & variant_column = column.getVariantColumn();

    if (column.getVariantInfo().variant_name_to_discriminator.contains(type_name)
        || column.addNewVariant(type, type_name))
    {
        const ColumnVariant::Discriminator discriminator = column.getVariantInfo().variant_name_to_discriminator.at(type_name);
        auto & variant = variant_column.getVariantByGlobalDiscriminator(discriminator);
        variant.insertFrom(src, n);
        variant_column.getOffsets().push_back(variant.size() - 1);
        variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(discriminator));
        return;
    }

    column.insertValueIntoSharedVariant(src, type, type_name, n);
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
    const IColumn * value,
    const IColumn * typed_value,
    const DataTypePtr & typed_value_type,
    ColumnDynamic & output,
    size_t num_rows,
    size_t max_parser_depth)
{
    const NullMap * metadata_nulls = nullptr;
    const ColumnString & metadata_strings = unwrapLeaf(metadata, metadata_nulls);

    const NullMap * value_nulls = nullptr;
    const ColumnString * value_strings = nullptr;
    if (value)
        value_strings = &unwrapLeaf(*value, value_nulls);

    const NullMap * typed_value_nulls = nullptr;
    const IColumn * typed_value_values = typed_value;
    DataTypePtr typed_value_inner_type;
    if (typed_value)
    {
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(typed_value))
        {
            typed_value_nulls = &nullable->getNullMapData();
            typed_value_values = &nullable->getNestedColumn();
        }
        typed_value_inner_type = removeNullable(typed_value_type);
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (metadata_nulls && (*metadata_nulls)[row])
        {
            output.insertDefault();
            continue;
        }

        const bool has_typed_value = typed_value_values && !(typed_value_nulls && (*typed_value_nulls)[row]);
        const bool has_value = value_strings && !(value_nulls && (*value_nulls)[row]);

        if (has_typed_value && has_value)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Malformed Parquet variant: row {} has both `value` and `typed_value` set, but a shredded "
                "value must be stored in exactly one of them", row);

        if (has_typed_value)
        {
            insertDynamicValueFrom(output, typed_value_inner_type, *typed_value_values, row);
            continue;
        }

        if (!has_value)
        {
            output.insertDefault();
            continue;
        }

        const std::string_view metadata_blob = metadata_strings.getDataAt(row);
        const std::string_view value_blob = value_strings->getDataAt(row);

        const Metadata parsed_metadata = parseMetadata(metadata_blob);
        const DecodeContext context{.metadata = parsed_metadata, .max_depth = max_parser_depth};
        decodeValueIntoDynamic(value_blob, 0, context, 0, output);
    }
}

}
