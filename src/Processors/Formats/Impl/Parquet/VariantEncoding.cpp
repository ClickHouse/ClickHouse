#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <base/extended_types.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <numeric>
#include <set>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB::Parquet
{

namespace
{

template <typename T>
void writeLittleEndianValue(T v, WriteBuffer & out)
{
    if constexpr (std::is_same_v<T, float>)
    {
        uint32_t u;
        std::memcpy(&u, &v, 4);
        writeLittleEndianValue(u, out);
    }
    else if constexpr (std::is_same_v<T, double>)
    {
        uint64_t u;
        std::memcpy(&u, &v, 8);
        writeLittleEndianValue(u, out);
    }
    else
    {
        make_unsigned_t<T> u;
        std::memcpy(&u, &v, sizeof(T));
        for (size_t i = 0; i < sizeof(T); ++i)
        {
            out.write(static_cast<char>(u & 0xFF));
            u >>= 8;
        }
    }
}

void writeUnsignedLittleEndian(uint64_t v, size_t size, WriteBuffer & out)
{
    for (size_t i = 0; i < size; ++i)
    {
        out.write(static_cast<char>(v & 0xFF));
        v >>= 8;
    }
}

void encodeString(std::string_view s, WriteBuffer & out)
{
    if (s.size() < 64)
    {
        out.write(static_cast<char>(1 | (s.size() << 2)));
        out.write(s.data(), s.size());
    }
    else
    {
        out.write(static_cast<char>(16 << 2));
        writeUnsignedLittleEndian(uint32_t(s.size()), 4, out);
        out.write(s.data(), s.size());
    }
}

using VariantRowRefInternal = VariantRowRef;

/// Resolve Nullable/LowCardinality/Dynamic wrappers to the concrete (column, type, row).
/// Returns false when the value is null.
bool resolveValue(const IColumn & column, const DataTypePtr & type, size_t row, VariantRowRef & out)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (nullable->isNullAt(row))
            return false;
        return resolveValue(nullable->getNestedColumn(), assert_cast<const DataTypeNullable &>(*type).getNestedType(), row, out);
    }
    if (typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        return resolveValue(column, assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType(), row, out);
    }
    if (const auto * dynamic = typeid_cast<const ColumnDynamic *>(&column))
    {
        const auto & variant_col = dynamic->getVariantColumn();
        const auto & variant_info = dynamic->getVariantInfo();
        auto discr = variant_col.globalDiscriminatorAt(row);
        if (discr == dynamic->getSharedVariantDiscriminator())
        {
            /// Value in the shared variant: (type, serialized value) binary; decode the type.
            auto value_data = dynamic->getSharedVariant().getDataAt(variant_col.offsetAt(row));
            ReadBufferFromMemory buf(value_data);
            /// If the shared-variant value is the Null type, it's a null.
            /// (decodeDataType is not cheap, but shared-variant rows are the rare path.)
            auto decoded_type = decodeDataType(buf);
            if (decoded_type->getTypeId() == TypeIndex::Nothing)
                return false;
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parquet VARIANT encoding of Dynamic values stored in the shared variant (more than {} dynamic types) is not supported", decoded_type->getName());
        }
        if (discr == ColumnVariant::NULL_DISCRIMINATOR)
            return false;
        out.column = &variant_col.getVariantByGlobalDiscriminator(discr);
        out.type = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants()[discr];
        out.row = variant_col.offsetAt(row);
        return resolveValue(*out.column, out.type, out.row, out);
    }
    out.column = &column;
    out.type = type;
    out.row = row;
    return true;
}

void collectFieldNames(const IColumn & column, const DataTypePtr & type, size_t row, std::set<String, std::less<>> & names)
{
    VariantRowRef value;
    if (!resolveValue(column, type, row, value))
        return;

    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(value.type.get()))
    {
        const auto & columns = assert_cast<const ColumnTuple &>(*value.column);
        if (tuple->hasExplicitNames())
            for (size_t i = 0; i < tuple->getElements().size(); ++i)
                names.insert(tuple->getElementNames()[i]);
        for (size_t i = 0; i < tuple->getElements().size(); ++i)
            collectFieldNames(columns.getColumn(i), tuple->getElement(i), value.row, names);
        return;
    }
    if (const auto * map = typeid_cast<const DataTypeMap *>(value.type.get()))
    {
        const auto & map_column = assert_cast<const ColumnMap &>(*value.column);
        const auto & nested = map_column.getNestedColumn();
        const auto & offsets = nested.getOffsets();
        const auto & keys = map_column.getNestedData().getColumn(0);
        const auto & values = map_column.getNestedData().getColumn(1);
        size_t begin = value.row > 0 ? offsets[value.row - 1] : 0;
        size_t end = offsets[value.row];
        for (size_t i = begin; i < end; ++i)
        {
            names.insert(String(keys.getDataAt(i)));
            collectFieldNames(values, map->getValueType(), i, names);
        }
        return;
    }
    if (const auto * array = typeid_cast<const DataTypeArray *>(value.type.get()))
    {
        const auto & array_column = assert_cast<const ColumnArray &>(*value.column);
        const auto & offsets = array_column.getOffsets();
        size_t begin = value.row > 0 ? offsets[value.row - 1] : 0;
        size_t end = offsets[value.row];
        for (size_t i = begin; i < end; ++i)
            collectFieldNames(array_column.getData(), array->getNestedType(), i, names);
        return;
    }
}

void encodeField(const IColumn & column, const DataTypePtr & type, size_t row, const std::vector<String> & dict, WriteBuffer & out);

/// Encode a variant object or array body: header, element count, field ids (objects), offsets,
/// then the values encoded into a side buffer.
void encodeComposite(
    bool is_object,
    const std::vector<std::pair<std::optional<uint32_t>, VariantRowRef>> & entries,
    const std::vector<String> & dict, WriteBuffer & out)
{
    if (entries.size() > 255)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Variant objects/arrays with more than 255 elements are not supported by the encoder");

    size_t field_id_size = dict.size() < 256 ? 1 : (dict.size() < 65536 ? 2 : 3);
    /// All offsets are 1 byte (is_large = 0).
    if (is_object)
        out.write(static_cast<char>((((field_id_size - 1) << 2) << 2) | 2));
    else
        out.write(static_cast<char>(3));
    writeUnsignedLittleEndian(entries.size(), 1, out);

    if (is_object)
        for (const auto & [field_id, v] : entries)
            writeUnsignedLittleEndian(*field_id, field_id_size, out);

    WriteBufferFromOwnString fields_buf;
    std::vector<uint32_t> offsets;
    offsets.reserve(entries.size() + 1);
    for (const auto & [field_id, v] : entries)
    {
        offsets.push_back(uint32_t(fields_buf.count()));
        encodeField(*v.column, v.type, v.row, dict, fields_buf);
    }
    offsets.push_back(uint32_t(fields_buf.count()));

    for (uint32_t o : offsets)
        out.write(static_cast<char>(o & 0xFF));
    std::string_view sv = fields_buf.stringView();
    out.write(sv.data(), sv.size());
}

template <typename T>
void encodeInt64(Int64 v, WriteBuffer & out)
{
    if (v >= -0x80 && v <= 0x7F)
    {
        out.write(static_cast<char>(3 << 2));
        out.write(static_cast<char>(v));
    }
    else if (v >= -0x8000 && v <= 0x7FFF)
    {
        out.write(static_cast<char>(4 << 2));
        writeLittleEndianValue(Int16(v), out);
    }
    else if (v >= -0x80000000LL && v <= 0x7FFFFFFF)
    {
        out.write(static_cast<char>(5 << 2));
        writeLittleEndianValue(Int32(v), out);
    }
    else
    {
        out.write(static_cast<char>(6 << 2));
        writeLittleEndianValue(v, out);
    }
}

void encodeField(const IColumn & column_, const DataTypePtr & type_, size_t row_, const std::vector<String> & dict, WriteBuffer & out)
{
    VariantRowRef value;
    if (!resolveValue(column_, type_, row_, value))
    {
        out.write('\0');
        return;
    }
    const IColumn & column = *value.column;
    const DataTypePtr & type = value.type;
    size_t row = value.row;

    switch (type->getTypeId())
    {
        case TypeIndex::Tuple:
        {
            const auto & tuple = assert_cast<const DataTypeTuple &>(*type);
            const auto & columns = assert_cast<const ColumnTuple &>(column);

            std::vector<std::pair<std::optional<uint32_t>, VariantRowRef>> entries;
            entries.reserve(tuple.getElements().size());
            if (tuple.hasExplicitNames())
            {
                const auto & names = tuple.getElementNames();
                std::vector<size_t> order(names.size());
                std::iota(order.begin(), order.end(), 0);
                std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return names[a] < names[b]; });
                for (size_t i : order)
                {
                    auto it = std::lower_bound(dict.begin(), dict.end(), names[i]);
                    entries.emplace_back(
                        uint32_t(it - dict.begin()),
                        VariantRowRef{&columns.getColumn(i), tuple.getElement(i), row});
                }
                encodeComposite(true, entries, dict, out);
            }
            else
            {
                for (size_t i = 0; i < tuple.getElements().size(); ++i)
                    entries.emplace_back(std::nullopt, VariantRowRef{&columns.getColumn(i), tuple.getElement(i), row});
                encodeComposite(false, entries, dict, out);
            }
            return;
        }
        case TypeIndex::Map:
        {
            const auto & map_column = assert_cast<const ColumnMap &>(column);
            const auto & nested = map_column.getNestedColumn();
            const auto & offsets = nested.getOffsets();
            const auto & keys = map_column.getNestedData().getColumn(0);
            const auto & values = map_column.getNestedData().getColumn(1);
            size_t begin = row > 0 ? offsets[row - 1] : 0;
            size_t end = offsets[row];

            const auto & map_type = assert_cast<const DataTypeMap &>(*type);
            std::vector<std::pair<String, VariantRowRef>> fields;
            fields.reserve(end - begin);
            for (size_t i = begin; i < end; ++i)
                fields.emplace_back(String(keys.getDataAt(i)), VariantRowRef{&values, map_type.getValueType(), i});
            std::sort(fields.begin(), fields.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

            std::vector<std::pair<std::optional<uint32_t>, VariantRowRef>> entries;
            entries.reserve(fields.size());
            for (const auto & [key, v] : fields)
            {
                auto it = std::lower_bound(dict.begin(), dict.end(), key);
                entries.emplace_back(uint32_t(it - dict.begin()), v);
            }
            encodeComposite(true, entries, dict, out);
            return;
        }
        case TypeIndex::Array:
        {
            const auto & array_column = assert_cast<const ColumnArray &>(column);
            const auto & offsets = array_column.getOffsets();
            size_t begin = row > 0 ? offsets[row - 1] : 0;
            size_t end = offsets[row];

            const auto & nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
            std::vector<std::pair<std::optional<uint32_t>, VariantRowRef>> entries;
            entries.reserve(end - begin);
            for (size_t i = begin; i < end; ++i)
                entries.emplace_back(std::nullopt, VariantRowRef{&array_column.getData(), nested_type, i});
            encodeComposite(false, entries, dict, out);
            return;
        }
        case TypeIndex::String:
        {
            encodeString(assert_cast<const ColumnString &>(column).getDataAt(row), out);
            return;
        }
        case TypeIndex::FixedString:
        {
            encodeString(assert_cast<const ColumnFixedString &>(column).getDataAt(row), out);
            return;
        }
        case TypeIndex::UInt8:
        {
            UInt8 v = assert_cast<const ColumnVector<UInt8> &>(column).getData()[row];
            if (type->getName() == "Bool")
            {
                out.write(static_cast<char>((v ? 1 : 2) << 2));
                return;
            }
            encodeInt64<UInt8>(v, out);
            return;
        }
        case TypeIndex::UInt16:
            encodeInt64<UInt16>(assert_cast<const ColumnVector<UInt16> &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt32:
            encodeInt64<UInt32>(assert_cast<const ColumnVector<UInt32> &>(column).getData()[row], out);
            return;
        case TypeIndex::UInt64:
        {
            UInt64 v = assert_cast<const ColumnVector<UInt64> &>(column).getData()[row];
            if (v <= 0x7FFFFFFFFFFFFFFF)
                encodeInt64<UInt64>(Int64(v), out);
            else
            {
                out.write(static_cast<char>(7 << 2));
                writeLittleEndianValue(Float64(v), out);
            }
            return;
        }
        case TypeIndex::Int8:
            encodeInt64<Int8>(assert_cast<const ColumnVector<Int8> &>(column).getData()[row], out);
            return;
        case TypeIndex::Int16:
            encodeInt64<Int16>(assert_cast<const ColumnVector<Int16> &>(column).getData()[row], out);
            return;
        case TypeIndex::Int32:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
            encodeInt64<Int32>(assert_cast<const ColumnVector<Int32> &>(column).getData()[row], out);
            return;
        case TypeIndex::Int64:
            encodeInt64<Int64>(assert_cast<const ColumnVector<Int64> &>(column).getData()[row], out);
            return;
        case TypeIndex::Float32:
        {
            out.write(static_cast<char>(14 << 2));
            writeLittleEndianValue(assert_cast<const ColumnVector<Float32> &>(column).getData()[row], out);
            return;
        }
        case TypeIndex::Float64:
        {
            out.write(static_cast<char>(7 << 2));
            writeLittleEndianValue(assert_cast<const ColumnVector<Float64> &>(column).getData()[row], out);
            return;
        }
        case TypeIndex::Date:
        {
            out.write(static_cast<char>(11 << 2));
            writeLittleEndianValue(Int32(assert_cast<const ColumnVector<UInt16> &>(column).getData()[row]), out);
            return;
        }
        case TypeIndex::Date32:
        {
            out.write(static_cast<char>(11 << 2));
            writeLittleEndianValue(assert_cast<const ColumnVector<Int32> &>(column).getData()[row], out);
            return;
        }
        case TypeIndex::DateTime:
        {
            out.write(static_cast<char>(12 << 2));
            writeLittleEndianValue(Int64(assert_cast<const ColumnVector<UInt32> &>(column).getData()[row]) * 1000000, out);
            return;
        }
        case TypeIndex::DateTime64:
        {
            const auto & data = assert_cast<const ColumnDecimal<DateTime64> &>(column).getData();
            UInt32 scale = assert_cast<const DataTypeDateTime64 &>(*type).getScale();
            if (scale <= 6)
            {
                Int64 micros = data[row].value;
                for (UInt32 s = scale; s < 6; ++s)
                    micros *= 10;
                out.write(static_cast<char>(12 << 2));
                writeLittleEndianValue(micros, out);
            }
            else
            {
                Int64 nanos = data[row].value;
                if (scale <= 9)
                {
                    for (UInt32 s = scale; s < 9; ++s)
                        nanos *= 10;
                }
                else
                {
                    for (UInt32 s = 9; s < scale; ++s)
                        nanos /= 10;
                }
                out.write(static_cast<char>(18 << 2));
                writeLittleEndianValue(nanos, out);
            }
            return;
        }
        case TypeIndex::Decimal32:
        {
            const auto & d = assert_cast<const ColumnDecimal<Decimal32> &>(column).getData()[row];
            out.write(static_cast<char>(8 << 2));
            out.write(static_cast<char>(assert_cast<const DataTypeDecimal<Decimal32> &>(*type).getScale()));
            writeLittleEndianValue(d.value, out);
            return;
        }
        case TypeIndex::Decimal64:
        {
            const auto & d = assert_cast<const ColumnDecimal<Decimal64> &>(column).getData()[row];
            out.write(static_cast<char>(9 << 2));
            out.write(static_cast<char>(assert_cast<const DataTypeDecimal<Decimal64> &>(*type).getScale()));
            writeLittleEndianValue(d.value, out);
            return;
        }
        case TypeIndex::Decimal128:
        {
            const auto & d = assert_cast<const ColumnDecimal<Decimal128> &>(column).getData()[row];
            out.write(static_cast<char>(10 << 2));
            out.write(static_cast<char>(assert_cast<const DataTypeDecimal<Decimal128> &>(*type).getScale()));
            writeLittleEndianValue(d.value, out);
            return;
        }
        case TypeIndex::UUID:
        {
            /// Variant UUID: 16 bytes big-endian (RFC 4122), same byte order as Parquet UUID.
            UUID uuid = assert_cast<const ColumnVector<UUID> &>(column).getData()[row];
            const auto * bytes = reinterpret_cast<const uint8_t *>(&uuid);
            char buf[16];
            if constexpr (std::endian::native == std::endian::little)
            {
                std::reverse_copy(bytes, bytes + 8, buf);
                std::reverse_copy(bytes + 8, bytes + 16, buf + 8);
            }
            else
            {
                std::memcpy(buf, bytes, 16);
            }
            out.write(static_cast<char>(20 << 2));
            out.write(buf, 16);
            return;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type '{}' is not supported for Parquet VARIANT encoding", type->getName());
    }
}

}

bool variantTypeContainsMap(const IDataType & type)
{
    if (typeid_cast<const DataTypeMap *>(&type) || typeid_cast<const DataTypeDynamic *>(&type))
        return true;
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        for (const auto & element : tuple->getElements())
            if (variantTypeContainsMap(*element))
                return true;
    }
    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return variantTypeContainsMap(*array->getNestedType());
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
        return variantTypeContainsMap(*nullable->getNestedType());
    return false;
}

std::optional<VariantRowRef> resolveVariantRow(const IColumn & column, const DataTypePtr & type, size_t row)
{
    VariantRowRef value;
    if (!resolveValue(column, type, row, value))
        return std::nullopt;
    return value;
}

void collectVariantFieldNames(const IColumn & column, const DataTypePtr & type, size_t row, std::vector<String> & out_sorted_names)
{
    std::set<String, std::less<>> name_set;
    collectFieldNames(column, type, row, name_set);
    out_sorted_names.assign(name_set.begin(), name_set.end());
}

std::string encodeVariantMetadata(const std::vector<String> & dict)
{
    /// Metadata: header (version 1, sorted_strings, offset_size), dictionary size, offsets, bytes.
    size_t dictionary_size = dict.size();
    size_t bytes_size = 0;
    for (const auto & name : dict)
        bytes_size += name.size();
    size_t offset_size = dictionary_size < 256 && bytes_size < 256 ? 1 : (dictionary_size < 65536 && bytes_size < 65536 ? 2 : 4);

    WriteBufferFromOwnString metadata_buf;
    uint8_t header = 0x01 | (1 << 4) | uint8_t((offset_size - 1) << 6);
    metadata_buf.write(static_cast<char>(header));
    writeUnsignedLittleEndian(dictionary_size, offset_size, metadata_buf);
    size_t offset = 0;
    for (const auto & name : dict)
    {
        writeUnsignedLittleEndian(offset, offset_size, metadata_buf);
        offset += name.size();
    }
    writeUnsignedLittleEndian(offset, offset_size, metadata_buf);
    for (const auto & name : dict)
        metadata_buf.write(name.data(), name.size());
    return metadata_buf.str();
}

void encodeVariantValueWithDict(const VariantRowRef & value, const std::vector<String> & dict, std::string & out_value)
{
    WriteBufferFromOwnString value_buf;
    encodeField(*value.column, value.type, value.row, dict, value_buf);
    out_value = value_buf.str();
}

void encodeVariantValue(const IColumn & column, const DataTypePtr & type, size_t row, std::string & out_metadata, std::string & out_value)
{
    std::vector<String> dict;
    collectVariantFieldNames(column, type, row, dict);
    out_metadata = encodeVariantMetadata(dict);

    VariantRowRef value{&column, type, row};
    encodeVariantValueWithDict(value, dict, out_value);
}

}
