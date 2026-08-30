#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVariant.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>
#include <cstring>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace DB::Parquet
{

namespace
{

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

template <typename T>
T readFixed(std::string_view data, size_t pos)
{
    return unalignedLoadLittleEndian<T>(data.data() + pos);
}

UInt32 readUnsigned(std::string_view data, size_t pos, UInt8 size)
{
    UInt32 res = 0;
    for (UInt8 i = 0; i < size; ++i)
        res |= static_cast<UInt32>(data[pos + i]) << (8 * i);
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
        const UInt32 begin = readUnsigned(blob, offsets_pos + size_t(id) * offset_size, offset_size);
        const UInt32 end = readUnsigned(blob, offsets_pos + (size_t(id) + 1) * offset_size, offset_size);
        return blob.substr(names_pos + begin, end - begin);
    }
};

Metadata parseMetadata(std::string_view blob)
{
    if (blob.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Empty Parquet variant metadata");

    const UInt8 header = UInt8(blob[0]);
    const UInt8 version = header & 0x0F;
    if (version != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Parquet variant metadata version {}", UInt16(version));

    Metadata res;
    res.blob = blob;
    res.offset_size = ((header >> 6) & 0x03) + 1;
    res.count = readUnsigned(blob, 1, res.offset_size);
    res.offsets_pos = 1 + res.offset_size;
    res.names_pos = res.offsets_pos + (size_t(res.count) + 1) * res.offset_size;
    return res;
}

struct DecodedValue
{
    DataTypePtr type;
    Field field;
};

DecodedValue decodeValue(std::string_view data, size_t pos, const Metadata & metadata, size_t depth);

DecodedValue decodePrimitive(std::string_view data, size_t pos, PrimitiveType type_id)
{
    switch (type_id)
    {
        case PrimitiveType::Null:
            return {nullptr, Field()};
        case PrimitiveType::True:
            return {DataTypeFactory::instance().get("Bool"), Field(UInt64(1))};
        case PrimitiveType::False:
            return {DataTypeFactory::instance().get("Bool"), Field(UInt64(0))};
        case PrimitiveType::Int8:
            return {std::make_shared<DataTypeInt8>(), Field(Int64(readFixed<Int8>(data, pos)))};
        case PrimitiveType::Int16:
            return {std::make_shared<DataTypeInt16>(), Field(Int64(readFixed<Int16>(data, pos)))};
        case PrimitiveType::Int32:
            return {std::make_shared<DataTypeInt32>(), Field(Int64(readFixed<Int32>(data, pos)))};
        case PrimitiveType::Int64:
            return {std::make_shared<DataTypeInt64>(), Field(readFixed<Int64>(data, pos))};
        case PrimitiveType::Float:
            return {std::make_shared<DataTypeFloat32>(), Field(Float64(readFixed<Float32>(data, pos)))};
        case PrimitiveType::Double:
            return {std::make_shared<DataTypeFloat64>(), Field(readFixed<Float64>(data, pos))};
        case PrimitiveType::Decimal4:
        {
            const UInt8 scale = UInt8(data[pos]);
            if (scale > 9)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet variant decimal4 has invalid scale {}", UInt16(scale));
            return {std::make_shared<DataTypeDecimal<Decimal32>>(9, scale),
                    Field(DecimalField<Decimal32>(Decimal32(readFixed<Int32>(data, pos + 1)), scale))};
        }
        case PrimitiveType::Decimal8:
        {
            const UInt8 scale = UInt8(data[pos]);
            if (scale > 18)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet variant decimal8 has invalid scale {}", UInt16(scale));
            return {std::make_shared<DataTypeDecimal<Decimal64>>(18, scale),
                    Field(DecimalField<Decimal64>(Decimal64(readFixed<Int64>(data, pos + 1)), scale))};
        }
        case PrimitiveType::Decimal16:
        {
            const UInt8 scale = UInt8(data[pos]);
            if (scale > 38)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet variant decimal16 has invalid scale {}", UInt16(scale));
            return {std::make_shared<DataTypeDecimal<Decimal128>>(38, scale),
                    Field(DecimalField<Decimal128>(Decimal128(readFixed<Int128>(data, pos + 1)), scale))};
        }
        case PrimitiveType::Date:
            return {std::make_shared<DataTypeDate32>(), Field(Int64(readFixed<Int32>(data, pos)))};
        case PrimitiveType::TimestampTZ:
            return {std::make_shared<DataTypeDateTime64>(6, "UTC"),
                    Field(DecimalField<DateTime64>(DateTime64(readFixed<Int64>(data, pos)), 6))};
        case PrimitiveType::TimestampNTZ:
            return {std::make_shared<DataTypeDateTime64>(6),
                    Field(DecimalField<DateTime64>(DateTime64(readFixed<Int64>(data, pos)), 6))};
        case PrimitiveType::TimestampNanosTZ:
            return {std::make_shared<DataTypeDateTime64>(9, "UTC"),
                    Field(DecimalField<DateTime64>(DateTime64(readFixed<Int64>(data, pos)), 9))};
        case PrimitiveType::TimestampNanosNTZ:
            return {std::make_shared<DataTypeDateTime64>(9),
                    Field(DecimalField<DateTime64>(DateTime64(readFixed<Int64>(data, pos)), 9))};
        case PrimitiveType::TimeNTZ:
            return {std::make_shared<DataTypeTime64>(6),
                    Field(DecimalField<Time64>(Time64(readFixed<Int64>(data, pos)), 6))};
        case PrimitiveType::UUID:
        {
            UUID uuid;
            memcpy(&uuid, data.data() + pos, 16);
            auto * bytes = reinterpret_cast<UInt8 *>(&uuid);
            if constexpr (std::endian::native == std::endian::little)
            {
                std::reverse(bytes, bytes + 8);
                std::reverse(bytes + 8, bytes + 16);
            }
            else
            {
                std::swap_ranges(bytes, bytes + 8, bytes + 8);
            }
            return {std::make_shared<DataTypeUUID>(), Field(uuid)};
        }
        case PrimitiveType::Binary:
        case PrimitiveType::String:
        {
            const UInt32 length = readFixed<UInt32>(data, pos);
            return {std::make_shared<DataTypeString>(), Field(String(data.substr(pos + 4, length)))};
        }
    }
}

DecodedValue decodeObject(std::string_view data, size_t pos, UInt8 value_header, const Metadata & metadata, size_t depth)
{
    const bool is_large = (value_header >> 4) & 0x01;
    const UInt8 id_size = ((value_header >> 2) & 0x03) + 1;
    const UInt8 offset_size = (value_header & 0x03) + 1;

    const UInt32 num_elements = readUnsigned(data, pos, is_large ? 4 : 1);
    const size_t ids_pos = pos + (is_large ? 4 : 1);
    const size_t offsets_pos = ids_pos + size_t(num_elements) * id_size;
    const size_t values_pos = offsets_pos + (size_t(num_elements) + 1) * offset_size;

    Map map;
    map.reserve(num_elements);
    for (UInt32 i = 0; i < num_elements; ++i)
    {
        const UInt32 field_id = readUnsigned(data, ids_pos + size_t(i) * id_size, id_size);
        const UInt32 offset = readUnsigned(data, offsets_pos + size_t(i) * offset_size, offset_size);
        DecodedValue element = decodeValue(data, values_pos + offset, metadata, depth + 1);
        map.push_back(Tuple{Field(String(metadata.getName(field_id))), std::move(element.field)});
    }

    return {
        std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
        std::make_shared<DataTypeDynamic>()),
        Field(std::move(map))
    };
}

DecodedValue decodeArray(std::string_view data, size_t pos, UInt8 value_header, const Metadata & metadata, size_t depth)
{
    const bool is_large = (value_header >> 2) & 0x01;
    const UInt8 offset_size = (value_header & 0x03) + 1;

    const UInt32 num_elements = readUnsigned(data, pos, is_large ? 4 : 1);
    const size_t offsets_pos = pos + (is_large ? 4 : 1);
    const size_t values_pos = offsets_pos + (size_t(num_elements) + 1) * offset_size;

    Array array;
    array.reserve(num_elements);
    for (UInt32 i = 0; i < num_elements; ++i)
    {
        const UInt32 offset = readUnsigned(data, offsets_pos + size_t(i) * offset_size, offset_size);
        array.push_back(decodeValue(data, values_pos + offset, metadata, depth + 1).field);
    }

    return {
        std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>()),
        Field(std::move(array))
    };
}

DecodedValue decodeValue(std::string_view data, size_t pos, const Metadata & metadata, size_t depth)
{
    const UInt8 header = UInt8(data[pos]);
    const BasicType basic_type = BasicType(header & 0x03);
    const UInt8 value_header = header >> 2;

    switch (basic_type)
    {
        case BasicType::Primitive:
            return decodePrimitive(data, pos + 1, PrimitiveType(value_header));
        case BasicType::ShortString:
            return {std::make_shared<DataTypeString>(), Field(String(data.substr(pos + 1, value_header)))};
        case BasicType::Object:
            return decodeObject(data, pos + 1, value_header, metadata, depth);
        default:
            return decodeArray(data, pos + 1, value_header, metadata, depth);
    }
}

void insertDynamicValue(ColumnDynamic & column, const DataTypePtr & type, const Field & value)
{
    const String type_name = type->getName();

    if (!column.getVariantInfo().variant_name_to_discriminator.contains(type_name)
        && !column.addNewVariant(type, type_name))
    {
        column.insert(value);
        return;
    }

    auto & variant_column = column.getVariantColumn();
    const ColumnVariant::Discriminator discriminator = column.getVariantInfo().variant_name_to_discriminator.at(type_name);
    variant_column.getVariantByGlobalDiscriminator(discriminator).insert(value);
    variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(discriminator));
    variant_column.getOffsets().push_back(variant_column.getVariantByGlobalDiscriminator(discriminator).size() - 1);
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

void decodeVariantColumn(const IColumn & metadata, const IColumn & value, ColumnDynamic & output, size_t num_rows)
{
    const NullMap * metadata_nulls = nullptr;
    const NullMap * value_nulls = nullptr;
    const ColumnString & metadata_strings = unwrapLeaf(metadata, metadata_nulls);
    const ColumnString & value_strings = unwrapLeaf(value, value_nulls);

    for (size_t row = 0; row < num_rows; ++row)
    {
        if ((value_nulls && (*value_nulls)[row]) || (metadata_nulls && (*metadata_nulls)[row]))
        {
            output.insertDefault();
            continue;
        }

        const std::string_view blob = metadata_strings.getDataAt(row);
        auto metadata_parsed = parseMetadata(blob);
        const DecodedValue decoded = decodeValue(value_strings.getDataAt(row), 0, metadata_parsed, 0);

        if (decoded.type)
            insertDynamicValue(output, decoded.type, decoded.field);
        else
            output.insertDefault();
    }
}

}
