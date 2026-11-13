#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/ListWithMemoryTracking.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Core/DecimalFunctions.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesCache.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/castColumn.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/checkStackSize.h>
#include <Common/isValidUTF8.h>
#include <Common/re2.h>
#include <base/arithmeticOverflow.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <memory>
#include <numeric>
#include <optional>

#include <fmt/format.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int TOO_DEEP_RECURSION;
    extern const int TYPE_MISMATCH;
}

namespace DB::Parquet
{

namespace
{

constexpr Int64 microseconds_per_second = 1'000'000;

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

template <typename T, std::endian endian = std::endian::little>
T readFixed(std::string_view data, size_t pos)
{
    checkRange(data, pos, sizeof(T));
    return unalignedLoadEndian<endian, T>(data.data() + pos);
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
    return unalignedLoadLittleEndian<UInt32>(data.data() + pos, size);
}

UInt8 unsignedSize(size_t value)
{
    if (value <= 0xFF)
        return 1;
    if (value <= 0xFFFF)
        return 2;
    if (value <= 0xFFFFFF)
        return 3;
    if (value <= 0xFFFFFFFF)
        return 4;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot write a Parquet variant: it has a part of {} bytes, more than 4 GiB", value);
}

void writeUnsigned(String & out, size_t value, UInt8 size)
{
    const size_t pos = out.size();
    out.resize(pos + size);
    unalignedStoreLittleEndian<size_t>(out.data() + pos, value, size);
}

void writeContainerHeader(String & out, BasicType basic_type, UInt8 value_header)
{
    out.push_back(static_cast<char>(UInt8(value_header << 2) | UInt8(basic_type)));
}

template <typename T, std::endian endian = std::endian::little>
void writeFixed(String & out, T value)
{
    const size_t pos = out.size();
    out.resize(pos + sizeof(T));
    unalignedStoreEndian<endian>(out.data() + pos, value);
}

void writePrimitiveHeader(String & out, PrimitiveType type)
{
    out.push_back(static_cast<char>(UInt8(type) << 2));
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
    bool escape_dots_in_json_keys;
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

}

/// Typed paths and `SKIP` clauses of a `JSON` column requested by the user.
class JSONPathsFilter
{
public:
    JSONPathsFilter(const DataTypeObject & type_, bool use_partial_match_)
        : type(type_)
        , sorted_paths_to_skip(type.getPathsToSkip().begin(), type.getPathsToSkip().end())
        , use_partial_match(use_partial_match_)
    {
        std::sort(sorted_paths_to_skip.begin(), sorted_paths_to_skip.end());
        for (const auto & regexp : type.getPathRegexpsToSkip())
            path_regexps_to_skip.emplace_back(regexp);
    }

    bool shouldSkip(const String & path) const
    {
        if (type.getPathsToSkip().contains(path))
            return true;

        auto it = std::lower_bound(sorted_paths_to_skip.begin(), sorted_paths_to_skip.end(), path);
        if (it != sorted_paths_to_skip.begin() && path.starts_with(*std::prev(it) + "."))
            return true;

        for (const auto & regexp : path_regexps_to_skip)
        {
            if (use_partial_match ? re2::RE2::PartialMatch(path, regexp) : re2::RE2::FullMatch(path, regexp))
                return true;
        }
        return false;
    }

    const DataTypePtr * findTypedPath(const String & path) const
    {
        const auto & typed_paths = type.getTypedPaths();
        auto it = typed_paths.find(path);
        return it == typed_paths.end() ? nullptr : &it->second;
    }

private:
    const DataTypeObject & type;
    VectorWithMemoryTracking<String> sorted_paths_to_skip;
    ListWithMemoryTracking<re2::RE2> path_regexps_to_skip;
    bool use_partial_match;
};

namespace
{

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

    static ObjectLayout deserialize(std::string_view data, size_t pos, UInt8 value_header)
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

    static void serialize(
        const VectorWithMemoryTracking<UInt32> & ids, const VectorWithMemoryTracking<size_t> & value_offsets, const String & values,
        String & out)
    {
        const bool is_large = ids.size() > 0xFF;
        const UInt8 id_size = unsignedSize(ids.empty() ? 0 : *std::max_element(ids.begin(), ids.end()));
        const UInt8 offset_size = unsignedSize(values.size());
        writeContainerHeader(out, BasicType::Object, static_cast<UInt8>((is_large << 4) | ((id_size - 1) << 2) | (offset_size - 1)));
        writeUnsigned(out, ids.size(), is_large ? 4 : 1);
        for (UInt32 id : ids)
            writeUnsigned(out, id, id_size);
        for (size_t offset : value_offsets)
            writeUnsigned(out, offset, offset_size);
        writeUnsigned(out, values.size(), offset_size);
        out.append(values);
    }
};

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

    static ArrayLayout deserialize(std::string_view data, size_t pos, UInt8 value_header)
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

    static void serialize(const VectorWithMemoryTracking<size_t> & value_offsets, const String & values, String & out)
    {
        const bool is_large = value_offsets.size() > 0xFF;
        const UInt8 offset_size = unsignedSize(values.size());
        writeContainerHeader(out, BasicType::Array, static_cast<UInt8>((is_large << 2) | (offset_size - 1)));
        writeUnsigned(out, value_offsets.size(), is_large ? 4 : 1);
        for (size_t offset : value_offsets)
            writeUnsigned(out, offset, offset_size);
        writeUnsigned(out, values.size(), offset_size);
        out.append(values);
    }
};

template <PrimitiveType type_id, typename T>
struct FixedSizePrimitive
{
    static void encode(String & out, T value)
    {
        writePrimitiveHeader(out, type_id);
        writeFixed<T>(out, value);
    }

    static T decode(std::string_view data, size_t pos)
    {
        return readFixed<T>(data, pos);
    }
};

using Int8Primitive = FixedSizePrimitive<PrimitiveType::Int8, Int8>;
using Int16Primitive = FixedSizePrimitive<PrimitiveType::Int16, Int16>;
using Int32Primitive = FixedSizePrimitive<PrimitiveType::Int32, Int32>;
using Int64Primitive = FixedSizePrimitive<PrimitiveType::Int64, Int64>;
using FloatPrimitive = FixedSizePrimitive<PrimitiveType::Float, Float32>;
using DoublePrimitive = FixedSizePrimitive<PrimitiveType::Double, Float64>;
using DatePrimitive = FixedSizePrimitive<PrimitiveType::Date, Int32>;
using TimestampPrimitive = FixedSizePrimitive<PrimitiveType::TimestampTZ, Int64>;
using TimestampNanosPrimitive = FixedSizePrimitive<PrimitiveType::TimestampNanosTZ, Int64>;
using TimePrimitive = FixedSizePrimitive<PrimitiveType::TimeNTZ, Int64>;

struct BooleanPrimitive
{
    static void encode(String & out, bool value)
    {
        writePrimitiveHeader(out, value ? PrimitiveType::True : PrimitiveType::False);
    }

    static bool decode(PrimitiveType type_id)
    {
        return type_id == PrimitiveType::True;
    }
};

template <PrimitiveType type_id, typename T>
struct DecimalPrimitive
{
    static void encode(String & out, T unscaled_value, UInt8 scale)
    {
        writePrimitiveHeader(out, type_id);
        out.push_back(static_cast<char>(scale));
        writeFixed<T>(out, unscaled_value);
    }

    static UInt8 decodeScale(std::string_view data, size_t pos)
    {
        return readByte(data, pos);
    }

    static T decode(std::string_view data, size_t pos)
    {
        return readFixed<T>(data, pos + 1);
    }
};

using Decimal4Primitive = DecimalPrimitive<PrimitiveType::Decimal4, Int32>;
using Decimal8Primitive = DecimalPrimitive<PrimitiveType::Decimal8, Int64>;
using Decimal16Primitive = DecimalPrimitive<PrimitiveType::Decimal16, Int128>;

struct UUIDPrimitive
{
    static void encode(String & out, const UUID & value)
    {
        writePrimitiveHeader(out, PrimitiveType::UUID);
        writeFixed<UInt64, std::endian::big>(out, UUIDHelpers::getHighBytes(value));
        writeFixed<UInt64, std::endian::big>(out, UUIDHelpers::getLowBytes(value));
    }

    static UUID decode(std::string_view data, size_t pos)
    {
        UUID res;
        UUIDHelpers::getHighBytes(res) = readFixed<UInt64, std::endian::big>(data, pos);
        UUIDHelpers::getLowBytes(res) = readFixed<UInt64, std::endian::big>(data, pos + sizeof(UInt64));
        return res;
    }
};

template <PrimitiveType type_id>
struct LongStringPrimitive
{
    static void encode(String & out, std::string_view value)
    {
        if (value.size() > std::numeric_limits<UInt32>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Cannot write a Parquet variant: it has a string of {} bytes, more than 4 GiB", value.size());
        writePrimitiveHeader(out, type_id);
        writeFixed<UInt32>(out, static_cast<UInt32>(value.size()));
        out.append(value);
    }

    static std::string_view decode(std::string_view data, size_t pos)
    {
        const UInt32 length = readFixed<UInt32>(data, pos);
        return readSlice(data, pos + sizeof(UInt32), length);
    }
};

using StringPrimitive = LongStringPrimitive<PrimitiveType::String>;
using BinaryPrimitive = LongStringPrimitive<PrimitiveType::Binary>;

struct ShortString
{
    static constexpr size_t max_size = 63;

    static void encode(String & out, std::string_view value)
    {
        writeContainerHeader(out, BasicType::ShortString, static_cast<UInt8>(value.size()));
        out.append(value);
    }

    static std::string_view decode(std::string_view data, size_t pos, UInt8 value_header)
    {
        return readSlice(data, pos, value_header);
    }
};

bool isObject(std::string_view data, size_t pos)
{
    return BasicType(readByte(data, pos) & 0x03) == BasicType::Object;
}

bool isArrayOfObjects(std::string_view data, size_t pos, UInt8 value_header)
{
    const ArrayLayout layout = ArrayLayout::deserialize(data, pos, value_header);
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
    const UInt8 scale = Decimal4Primitive::decodeScale(data, pos + 1);
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
        case PrimitiveType::False:
            assert_cast<ColumnUInt8 &>(target).insertValue(BooleanPrimitive::decode(type_id));
            return;
        case PrimitiveType::Int8:
            assert_cast<ColumnInt8 &>(target).insertValue(Int8Primitive::decode(data, pos));
            return;
        case PrimitiveType::Int16:
            assert_cast<ColumnInt16 &>(target).insertValue(Int16Primitive::decode(data, pos));
            return;
        case PrimitiveType::Int32:
            assert_cast<ColumnInt32 &>(target).insertValue(Int32Primitive::decode(data, pos));
            return;
        case PrimitiveType::Int64:
            assert_cast<ColumnInt64 &>(target).insertValue(Int64Primitive::decode(data, pos));
            return;
        case PrimitiveType::Float:
            assert_cast<ColumnFloat32 &>(target).insertValue(FloatPrimitive::decode(data, pos));
            return;
        case PrimitiveType::Double:
            assert_cast<ColumnFloat64 &>(target).insertValue(DoublePrimitive::decode(data, pos));
            return;
        case PrimitiveType::Decimal4:
            assert_cast<ColumnDecimal<Decimal32> &>(target).insertValue(Decimal32(Decimal4Primitive::decode(data, pos)));
            return;
        case PrimitiveType::Decimal8:
            assert_cast<ColumnDecimal<Decimal64> &>(target).insertValue(Decimal64(Decimal8Primitive::decode(data, pos)));
            return;
        case PrimitiveType::Decimal16:
            assert_cast<ColumnDecimal<Decimal128> &>(target).insertValue(Decimal128(Decimal16Primitive::decode(data, pos)));
            return;
        case PrimitiveType::Date:
            assert_cast<ColumnDate32 &>(target).insertValue(DatePrimitive::decode(data, pos));
            return;
        case PrimitiveType::TimestampTZ:
        case PrimitiveType::TimestampNTZ:
            assert_cast<ColumnDecimal<DateTime64> &>(target).insertValue(DateTime64(TimestampPrimitive::decode(data, pos)));
            return;
        case PrimitiveType::TimestampNanosTZ:
        case PrimitiveType::TimestampNanosNTZ:
            assert_cast<ColumnDecimal<DateTime64> &>(target).insertValue(DateTime64(TimestampNanosPrimitive::decode(data, pos)));
            return;
        case PrimitiveType::TimeNTZ:
            assert_cast<ColumnDecimal<Time64> &>(target).insertValue(Time64(TimePrimitive::decode(data, pos)));
            return;
        case PrimitiveType::UUID:
            assert_cast<ColumnUUID &>(target).insertValue(UUIDPrimitive::decode(data, pos));
            return;
        case PrimitiveType::Binary:
        {
            const std::string_view value = BinaryPrimitive::decode(data, pos);
            assert_cast<ColumnString &>(target).insertData(value.data(), value.size());
            return;
        }
        case PrimitiveType::String:
        {
            const std::string_view value = StringPrimitive::decode(data, pos);
            assert_cast<ColumnString &>(target).insertData(value.data(), value.size());
            return;
        }
        case PrimitiveType::Null:
            break;
    }

    throw Exception(
        ErrorCodes::INCORRECT_DATA, "Malformed Parquet variant: unknown primitive type id {}", UInt16(type_id));
}

void decodeValueIntoTypedPath(
    std::string_view data, size_t pos, const DecodeContext & context, size_t depth, const DataTypePtr & typed_type, IColumn & target)
{
    const String type_name = getValueTypeName(data, pos);
    if (type_name == typed_type->getName())
    {
        decodeValueIntoColumn(data, pos, context, depth, target);
        return;
    }

    /// TODO: Add a fast path for simple conversions (number -> number, string -> number,
    /// number -> string, etc.) that doesn't create a column and call castColumn: that is too
    /// expensive to do per single value.
    const DataTypePtr type = getDataTypesCache().getType(type_name);
    auto value_column = type->createColumn();
    decodeValueIntoColumn(data, pos, context, depth, *value_column);
    const ColumnPtr casted = castColumn({std::move(value_column), type, ""}, typed_type);
    target.insertFrom(*casted, 0);
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
    const JSONPathsFilter * filter,
    size_t prev_size,
    VectorWithMemoryTracking<SharedDataValue> & shared_data_values)
{
    const ObjectLayout layout = ObjectLayout::deserialize(data, pos, value_header);

    for (UInt32 i = 0; i < layout.num_elements; ++i)
    {
        String name(context.metadata.getName(layout.fieldId(data, i)));
        if (context.escape_dots_in_json_keys)
            name = escapeDotInJSONKey(name);
        String path = is_root ? std::move(name) : prefix + "." + name;
        if (filter && filter->shouldSkip(path))
            continue;

        const size_t value_pos = layout.valuePos(data, i);
        const UInt8 header = readByte(data, value_pos);

        /// Like in the `JSON` type, a null is equivalent to the absence of the path.
        if (header == UInt8(PrimitiveType::Null) << 2)
            continue;

        if (const DataTypePtr * typed_type = filter ? filter->findTypedPath(path) : nullptr)
        {
            auto & typed_column = *column.getTypedPaths().at(path);
            if (typed_column.size() > prev_size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Parquet variant object has the path '{}' more than once after flattening nested objects", path);
            decodeValueIntoTypedPath(data, value_pos, context, depth + 1, *typed_type, typed_column);
            continue;
        }

        if (BasicType(header & 0x03) == BasicType::Object)
        {
            checkDepth(context, depth + 1);
            insertObjectPaths(
                data, value_pos + 1, header >> 2, context, depth + 1, path, false, column, filter, prev_size, shared_data_values);
            continue;
        }

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
    std::string_view data,
    size_t pos,
    UInt8 value_header,
    const DecodeContext & context,
    size_t depth,
    ColumnObject & column,
    const JSONPathsFilter * filter = nullptr)
{
    const size_t prev_size = column.size();

    VectorWithMemoryTracking<SharedDataValue> shared_data_values;
    insertObjectPaths(data, pos, value_header, context, depth, "", true, column, filter, prev_size, shared_data_values);

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
        /// Keep only the value being serialized, not all values of all shared data paths.
        if (!tmp_dynamic.empty())
            tmp_dynamic.popBack(1);
        decodeValueIntoDynamic(data, value.pos, context, value.depth, tmp_dynamic);
        ColumnObject::serializePathAndValueIntoSharedData(
            shared_data_paths, shared_data_values_column, value.path, tmp_dynamic, 0);
    }
    column.getSharedDataOffsets().push_back(shared_data_paths->size());

    for (auto & [_, typed_column] : column.getTypedPaths())
    {
        if (typed_column->size() == prev_size)
            typed_column->insertDefault();
    }

    for (auto & [_, dynamic_column] : column.getDynamicPathsPtrs())
    {
        if (dynamic_column->size() == prev_size)
            dynamic_column->insertDefault();
    }
}

void decodeArrayIntoColumn(
    std::string_view data, size_t pos, UInt8 value_header, const DecodeContext & context, size_t depth, IColumn & target)
{
    const ArrayLayout layout = ArrayLayout::deserialize(data, pos, value_header);

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
            const std::string_view value = ShortString::decode(data, pos + 1, value_header);
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

struct JSONNode
{
    MapWithMemoryTracking<String, size_t> children;
    std::optional<String> value;
};

class VariantEncoder
{
public:
    VariantEncoder(const String & column_name_, bool escape_dots_in_json_keys_, size_t max_json_depth_)
        : column_name(column_name_), escape_dots_in_json_keys(escape_dots_in_json_keys_), max_json_depth(max_json_depth_)
    {
    }

    void reset()
    {
        key_ids.clear();
        keys.clear();
    }

    void writeMetadata(String & out) const
    {
        size_t names_size = 0;
        for (const auto & key : keys)
            names_size += key.size();

        const UInt8 offset_size = unsignedSize(std::max(keys.size(), names_size));
        out.push_back(static_cast<char>(1 | ((offset_size - 1) << 6)));
        writeUnsigned(out, keys.size(), offset_size);
        size_t offset = 0;
        writeUnsigned(out, offset, offset_size);
        for (const auto & key : keys)
        {
            offset += key.size();
            writeUnsigned(out, offset, offset_size);
        }
        for (const auto & key : keys)
            out.append(key);
    }

    void encode(const IColumn & column, const IDataType & type, size_t row, String & out)
    {
        checkStackSize();

        switch (type.getTypeId())
        {
            case TypeIndex::Nullable:
            {
                const auto & nullable = assert_cast<const ColumnNullable &>(column);
                if (nullable.isNullAt(row))
                    writePrimitiveHeader(out, PrimitiveType::Null);
                else
                    encode(nullable.getNestedColumn(), *assert_cast<const DataTypeNullable &>(type).getNestedType(), row, out);
                return;
            }
            case TypeIndex::LowCardinality:
            {
                const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(column);
                encode(
                    *low_cardinality.getDictionary().getNestedColumn(),
                    *assert_cast<const DataTypeLowCardinality &>(type).getDictionaryType(),
                    low_cardinality.getIndexAt(row),
                    out);
                return;
            }
            case TypeIndex::UInt8:
                if (type.getName() == "Bool")
                {
                    BooleanPrimitive::encode(out, assert_cast<const ColumnUInt8 &>(column).getData()[row] != 0);
                    return;
                }
                Int16Primitive::encode(out, assert_cast<const ColumnUInt8 &>(column).getData()[row]);
                return;
            case TypeIndex::Int8:
                Int8Primitive::encode(out, assert_cast<const ColumnInt8 &>(column).getData()[row]);
                return;
            case TypeIndex::Int16:
                Int16Primitive::encode(out, assert_cast<const ColumnInt16 &>(column).getData()[row]);
                return;
            case TypeIndex::UInt16:
                Int32Primitive::encode(out, assert_cast<const ColumnUInt16 &>(column).getData()[row]);
                return;
            case TypeIndex::Int32:
                Int32Primitive::encode(out, assert_cast<const ColumnInt32 &>(column).getData()[row]);
                return;
            case TypeIndex::UInt32:
                Int64Primitive::encode(out, assert_cast<const ColumnUInt32 &>(column).getData()[row]);
                return;
            case TypeIndex::Int64:
                Int64Primitive::encode(out, assert_cast<const ColumnInt64 &>(column).getData()[row]);
                return;
            case TypeIndex::UInt64:
            {
                const UInt64 value = assert_cast<const ColumnUInt64 &>(column).getData()[row];
                if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot write the value {} of type UInt64 into the Parquet variant column '{}': the widest variant "
                        "integer is a signed 64-bit integer",
                        value, column_name);
                Int64Primitive::encode(out, static_cast<Int64>(value));
                return;
            }
            case TypeIndex::Float32:
                FloatPrimitive::encode(out, assert_cast<const ColumnFloat32 &>(column).getData()[row]);
                return;
            case TypeIndex::Float64:
                DoublePrimitive::encode(out, assert_cast<const ColumnFloat64 &>(column).getData()[row]);
                return;
            case TypeIndex::Decimal32:
                encodeDecimal<Decimal4Primitive, Decimal32>(column, type, row, out);
                return;
            case TypeIndex::Decimal64:
                encodeDecimal<Decimal8Primitive, Decimal64>(column, type, row, out);
                return;
            case TypeIndex::Decimal128:
                encodeDecimal<Decimal16Primitive, Decimal128>(column, type, row, out);
                return;
            case TypeIndex::Date:
                DatePrimitive::encode(out, assert_cast<const ColumnDate &>(column).getData()[row]);
                return;
            case TypeIndex::Date32:
                DatePrimitive::encode(out, assert_cast<const ColumnDate32 &>(column).getData()[row]);
                return;
            case TypeIndex::DateTime:
                TimestampPrimitive::encode(out, Int64(assert_cast<const ColumnDateTime &>(column).getData()[row]) * microseconds_per_second);
                return;
            case TypeIndex::DateTime64:
            {
                const UInt32 scale = assert_cast<const DataTypeDateTime64 &>(type).getScale();
                const Int64 value = assert_cast<const ColumnDateTime64 &>(column).getData()[row].value;
                const UInt32 target_scale = scale <= 6 ? 6 : 9;
                Int64 scaled = 0;
                if (common::mulOverflow(value, DecimalUtils::scaleMultiplier<Int64>(target_scale - scale), scaled))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot write the value {} of type {} into the Parquet variant column '{}': it is out of the range of a "
                        "variant timestamp with {} digits of fractional seconds",
                        value, type.getName(), column_name, target_scale);
                if (scale <= 6)
                    TimestampPrimitive::encode(out, scaled);
                else
                    TimestampNanosPrimitive::encode(out, scaled);
                return;
            }
            case TypeIndex::Time:
                encodeTime(Int64(assert_cast<const ColumnTime &>(column).getData()[row]) * microseconds_per_second, type, out);
                return;
            case TypeIndex::Time64:
            {
                const UInt32 scale = assert_cast<const DataTypeTime64 &>(type).getScale();
                if (scale > 6)
                    throwUnsupported(type);
                encodeTime(assert_cast<const ColumnTime64 &>(column).getData()[row].value * DecimalUtils::scaleMultiplier<Int64>(6 - scale), type, out);
                return;
            }
            case TypeIndex::String:
                encodeString(column.getDataAt(row), out);
                return;
            case TypeIndex::FixedString:
                BinaryPrimitive::encode(out, column.getDataAt(row));
                return;
            case TypeIndex::Enum8:
                encodeString(assert_cast<const DataTypeEnum8 &>(type).getNameForValue(assert_cast<const ColumnInt8 &>(column).getData()[row]), out);
                return;
            case TypeIndex::Enum16:
                encodeString(assert_cast<const DataTypeEnum16 &>(type).getNameForValue(assert_cast<const ColumnInt16 &>(column).getData()[row]), out);
                return;
            case TypeIndex::UUID:
            {
                UUIDPrimitive::encode(out, assert_cast<const ColumnUUID &>(column).getData()[row]);
                return;
            }
            case TypeIndex::Array:
            {
                const auto & array = assert_cast<const ColumnArray &>(column);
                const auto & nested_type = *assert_cast<const DataTypeArray &>(type).getNestedType();
                const auto & offsets = array.getOffsets();
                String values;
                VectorWithMemoryTracking<size_t> value_offsets;
                for (size_t i = offsets[row - 1]; i < offsets[row]; ++i)
                {
                    value_offsets.push_back(values.size());
                    encode(array.getData(), nested_type, i, values);
                }
                ArrayLayout::serialize(value_offsets, values, out);
                return;
            }
            case TypeIndex::Tuple:
            {
                const auto & tuple_type = assert_cast<const DataTypeTuple &>(type);
                const auto & tuple = assert_cast<const ColumnTuple &>(column);
                const auto & element_types = tuple_type.getElements();
                String values;
                VectorWithMemoryTracking<size_t> value_offsets;
                if (!tuple_type.hasExplicitNames())
                {
                    for (size_t i = 0; i < element_types.size(); ++i)
                    {
                        value_offsets.push_back(values.size());
                        encode(tuple.getColumn(i), *element_types[i], row, values);
                    }
                    ArrayLayout::serialize(value_offsets, values, out);
                    return;
                }

                const auto & names = tuple_type.getElementNames();
                VectorWithMemoryTracking<size_t> order(names.size());
                std::iota(order.begin(), order.end(), 0);
                std::sort(order.begin(), order.end(), [&](size_t left, size_t right) { return names[left] < names[right]; });
                VectorWithMemoryTracking<UInt32> ids;
                for (size_t i : order)
                {
                    ids.push_back(keyId(names[i]));
                    value_offsets.push_back(values.size());
                    encode(tuple.getColumn(i), *element_types[i], row, values);
                }
                ObjectLayout::serialize(ids, value_offsets, values, out);
                return;
            }
            case TypeIndex::Map:
            {
                const auto & map_type = assert_cast<const DataTypeMap &>(type);
                if (!isStringOrFixedString(removeLowCardinality(map_type.getKeyType())))
                    throwUnsupported(type);

                const auto & nested = assert_cast<const ColumnMap &>(column).getNestedColumn();
                const auto & map_keys = assert_cast<const ColumnTuple &>(nested.getData()).getColumn(0);
                const auto & map_values = assert_cast<const ColumnTuple &>(nested.getData()).getColumn(1);
                const auto & offsets = nested.getOffsets();

                VectorWithMemoryTracking<size_t> order(offsets[row] - offsets[row - 1]);
                std::iota(order.begin(), order.end(), offsets[row - 1]);
                std::sort(order.begin(), order.end(), [&](size_t left, size_t right) { return map_keys.getDataAt(left) < map_keys.getDataAt(right); });

                String values;
                VectorWithMemoryTracking<size_t> value_offsets;
                VectorWithMemoryTracking<UInt32> ids;
                for (size_t j = 0; j < order.size(); ++j)
                {
                    const std::string_view key = map_keys.getDataAt(order[j]);
                    if (j != 0 && key == map_keys.getDataAt(order[j - 1]))
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Cannot write a map with the duplicate key '{}' into the Parquet variant column '{}': "
                            "the keys of a variant object must be unique",
                            key, column_name);
                    ids.push_back(keyId(key));
                    value_offsets.push_back(values.size());
                    encode(map_values, *map_type.getValueType(), order[j], values);
                }
                ObjectLayout::serialize(ids, value_offsets, values, out);
                return;
            }
            case TypeIndex::Variant:
            {
                if (type.getCustomName())
                    throwUnsupported(type);
                const auto & variant = assert_cast<const ColumnVariant &>(column);
                const auto discriminator = variant.globalDiscriminatorAt(row);
                if (discriminator == ColumnVariant::NULL_DISCRIMINATOR)
                {
                    writePrimitiveHeader(out, PrimitiveType::Null);
                    return;
                }
                encode(
                    variant.getVariantByGlobalDiscriminator(discriminator),
                    *assert_cast<const DataTypeVariant &>(type).getVariant(discriminator),
                    variant.offsetAt(row),
                    out);
                return;
            }
            case TypeIndex::Dynamic:
                encodeDynamic(assert_cast<const ColumnDynamic &>(column), row, out);
                return;
            case TypeIndex::Object:
                encodeJSON(assert_cast<const ColumnObject &>(column), assert_cast<const DataTypeObject &>(type), row, out);
                return;
            default:
                throwUnsupported(type);
        }
    }

private:
    [[noreturn]] void throwUnsupported(const IDataType & type) const
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Cannot write a value of type {} into the Parquet variant column '{}'", type.getName(), column_name);
    }

    UInt32 keyId(std::string_view key)
    {
        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(key.data()), key.size()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot write the object key '{}' into the Parquet variant column '{}': the keys of a variant object must be "
                "valid UTF-8",
                key, column_name);
        auto [it, inserted] = key_ids.try_emplace(String(key), static_cast<UInt32>(keys.size()));
        if (inserted)
            keys.push_back(it->first);
        return it->second;
    }

    template <typename Primitive, typename DecimalType>
    static void encodeDecimal(const IColumn & column, const IDataType & type, size_t row, String & out)
    {
        Primitive::encode(
            out, assert_cast<const ColumnDecimal<DecimalType> &>(column).getData()[row].value, static_cast<UInt8>(getDecimalScale(type)));
    }

    void encodeTime(Int64 micros, const IDataType & type, String & out) const
    {
        if (micros < 0 || micros >= 86'400'000'000LL)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot write a value of type {} into the Parquet variant column '{}': a variant time is a time of day, "
                "but the value is {} microseconds",
                type.getName(), column_name, micros);
        TimePrimitive::encode(out, micros);
    }

    static void encodeString(std::string_view value, String & out)
    {
        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(value.data()), value.size()))
            BinaryPrimitive::encode(out, value);
        else if (value.size() <= ShortString::max_size)
            ShortString::encode(out, value);
        else
            StringPrimitive::encode(out, value);
    }

    void encodeDynamic(const ColumnDynamic & dynamic, size_t row, String & out)
    {
        const auto & variant = dynamic.getVariantColumn();
        const auto discriminator = variant.globalDiscriminatorAt(row);
        if (discriminator == ColumnVariant::NULL_DISCRIMINATOR)
        {
            writePrimitiveHeader(out, PrimitiveType::Null);
            return;
        }

        if (discriminator == dynamic.getSharedVariantDiscriminator())
        {
            ReadBufferFromMemory buf(dynamic.getSharedVariant().getDataAt(variant.offsetAt(row)));
            const DataTypePtr value_type = decodeDataType(buf);
            auto value_column = value_type->createColumn();
            value_type->getDefaultSerialization()->deserializeBinary(*value_column, buf, FormatSettings{});
            encode(*value_column, *value_type, 0, out);
            return;
        }

        encode(
            variant.getVariantByGlobalDiscriminator(discriminator),
            *assert_cast<const DataTypeVariant &>(*dynamic.getVariantInfo().variant_type).getVariant(discriminator),
            variant.offsetAt(row),
            out);
    }

    void encodeJSON(const ColumnObject & object, const DataTypeObject & type, size_t row, String & out)
    {
        VectorWithMemoryTracking<JSONNode> nodes(1);
        for (auto it = ColumnObject::SortedPathsIterator(object, row, /*skip_typed_nulls_=*/true); !it.end(); it.next())
        {
            const auto path_info = it.getCurrentPathInfo();
            if (path_info.column->isNullAt(path_info.row))
                continue;

            if (max_json_depth != 0
                && static_cast<size_t>(std::count(path_info.path.begin(), path_info.path.end(), '.')) >= max_json_depth)
                throw Exception(
                    ErrorCodes::TOO_DEEP_RECURSION,
                    "Cannot write a JSON value into the Parquet variant column '{}': the path '{}' is nested deeper than {} levels",
                    column_name, path_info.path, max_json_depth);

            size_t node = 0;
            size_t begin = 0;
            while (true)
            {
                if (nodes[node].value)
                    throwJSONPathConflict(path_info.path);
                const size_t end = path_info.path.find('.', begin);
                String key(path_info.path.substr(begin, end == std::string_view::npos ? end : end - begin));
                if (escape_dots_in_json_keys)
                    key = unescapeDotInJSONKey(key);
                const auto [child_it, inserted] = nodes[node].children.try_emplace(std::move(key), nodes.size());
                const size_t child = child_it->second;
                if (inserted)
                    nodes.emplace_back();
                node = child;
                if (end == std::string_view::npos)
                    break;
                begin = end + 1;
            }
            if (nodes[node].value || !nodes[node].children.empty())
                throwJSONPathConflict(path_info.path);

            auto & value = nodes[node].value.emplace();
            if (path_info.type == ColumnObject::SortedPathsIterator::PathType::TYPED)
                encode(*path_info.column, *type.getTypedPaths().at(String(path_info.path)), path_info.row, value);
            else
                encodeDynamic(assert_cast<const ColumnDynamic &>(*path_info.column), path_info.row, value);
        }
        writeJSONNode(nodes, 0, out);
    }

    void writeJSONNode(const VectorWithMemoryTracking<JSONNode> & nodes, size_t node_index, String & out)
    {
        checkStackSize();
        const JSONNode & node = nodes[node_index];
        if (node.value)
        {
            out.append(*node.value);
            return;
        }

        String values;
        VectorWithMemoryTracking<size_t> value_offsets;
        VectorWithMemoryTracking<UInt32> ids;
        for (const auto & [name, child] : node.children)
        {
            ids.push_back(keyId(name));
            value_offsets.push_back(values.size());
            writeJSONNode(nodes, child, values);
        }
        ObjectLayout::serialize(ids, value_offsets, values, out);
    }

    [[noreturn]] void throwJSONPathConflict(std::string_view path) const
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot write a JSON value into the Parquet variant column '{}': the path '{}' is both a value and a prefix of another path",
            column_name, path);
    }

    const String & column_name;
    const bool escape_dots_in_json_keys;
    const size_t max_json_depth;
    UnorderedMapWithMemoryTracking<String, UInt32> key_ids;
    VectorWithMemoryTracking<std::string_view> keys;
};

}

VariantValueDecoder::VariantValueDecoder(DataTypePtr output_type_, String column_name_, const FormatSettings & format_settings)
    : output_type(std::move(output_type_))
    , column_name(std::move(column_name_))
    , max_depth(format_settings.max_parser_depth)
    , escape_dots_in_json_keys(format_settings.json.json_type_escape_dots_in_keys)
{
    const DataTypePtr inner_type = removeNullable(output_type);
    if (const auto * object_type = typeid_cast<const DataTypeObject *>(inner_type.get()))
        filter = std::make_shared<const JSONPathsFilter>(*object_type, format_settings.json.type_json_use_partial_match_to_skip_paths_by_regexp);
    else
        chassert(isDynamic(inner_type));
}

void VariantValueDecoder::decode(std::string_view metadata_blob, std::string_view value_blob, IColumn & output) const
{
    const Metadata parsed_metadata = parseMetadata(metadata_blob);
    const DecodeContext context{.metadata = parsed_metadata, .max_depth = max_depth, .escape_dots_in_json_keys = escape_dots_in_json_keys};

    /// `Nullable(JSON)`: values are decoded into the nested column, nulls (of the whole group or a variant
    /// null) go through `output.insertDefault()`, which inserts NULL into a nullable column.
    auto * output_nullable = typeid_cast<ColumnNullable *>(&output);
    IColumn & output_inner = output_nullable ? output_nullable->getNestedColumn() : output;

    if (!filter)
    {
        decodeValueIntoDynamic(value_blob, 0, context, 0, assert_cast<ColumnDynamic &>(output_inner));
        return;
    }

    const UInt8 header = readByte(value_blob, 0);
    if (BasicType(header & 0x03) != BasicType::Object)
    {
        const String type_name = getValueTypeName(value_blob, 0);
        /// A variant null is an empty object, the same as a null of the whole column.
        if (type_name.empty())
        {
            output.insertDefault();
            return;
        }
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
            "Cannot read Parquet variant column '{}' as {}: it contains a value of type {}, but only objects can be "
            "read as JSON. Read the column as Dynamic instead",
            column_name, output_type->getName(), type_name);
    }

    checkDepth(context, 0);
    decodeObjectIntoJSON(value_blob, 1, header >> 2, context, 0, assert_cast<ColumnObject &>(output_inner), filter.get());
    if (output_nullable)
        output_nullable->getNullMapData().push_back(false);
}

void decodeVariantColumn(
    const IColumn & metadata,
    const IColumn & value,
    IColumn & output,
    const DataTypePtr & output_type,
    const String & column_name,
    size_t num_rows,
    const FormatSettings & format_settings)
{
    const NullMap * metadata_nulls = nullptr;
    const ColumnString & metadata_strings = unwrapLeaf(metadata, metadata_nulls);

    const NullMap * value_nulls = nullptr;
    const ColumnString & value_strings = unwrapLeaf(value, value_nulls);

    const VariantValueDecoder decoder(output_type, column_name, format_settings);

    for (size_t row = 0; row < num_rows; ++row)
    {
        const std::string_view metadata_blob = metadata_strings.getDataAt(row);
        const std::string_view value_blob = value_strings.getDataAt(row);

        /// `metadata` and `value` are one variant value, so they are null together, when the whole
        /// variant is null. With `input_format_null_as_default` the leaves are read as non-nullable
        /// and a null arrives as an empty blob, which is never a valid encoding (both start with a
        /// header byte). A null in just one of the leaves is malformed: in the unshredded layout,
        /// which is the only one read here, both fields are required.
        const bool metadata_missing = (metadata_nulls && (*metadata_nulls)[row]) || metadata_blob.empty();
        const bool value_missing = (value_nulls && (*value_nulls)[row]) || value_blob.empty();
        if (metadata_missing && value_missing)
        {
            output.insertDefault();
            continue;
        }
        if (metadata_missing || value_missing)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Malformed Parquet variant column '{}': a row has {} but no {}",
                column_name, metadata_missing ? "`value`" : "`metadata`", metadata_missing ? "`metadata`" : "`value`");

        decoder.decode(metadata_blob, value_blob, output);
    }
}

void encodeVariantColumn(
    const IColumn & column,
    const DataTypePtr & type,
    const String & column_name,
    bool escape_dots_in_json_keys,
    size_t max_json_depth,
    ColumnString & metadata,
    ColumnString & value,
    PaddedPODArray<UInt8> * null_map)
{
    const size_t num_rows = column.size();
    const bool nullable_root = null_map && (type->getTypeId() == TypeIndex::Dynamic || type->getTypeId() == TypeIndex::Variant);
    if (null_map)
        null_map->resize_fill(num_rows, 0);
    metadata.reserve(num_rows);
    value.reserve(num_rows);

    VariantEncoder encoder(column_name, escape_dots_in_json_keys, max_json_depth);
    String metadata_blob;
    String value_blob;
    for (size_t row = 0; row < num_rows; ++row)
    {
        if (nullable_root && column.isNullAt(row))
        {
            (*null_map)[row] = 1;
            metadata.insertDefault();
            value.insertDefault();
            continue;
        }

        encoder.reset();
        metadata_blob.clear();
        value_blob.clear();
        encoder.encode(column, *type, row, value_blob);
        encoder.writeMetadata(metadata_blob);
        metadata.insertData(metadata_blob.data(), metadata_blob.size());
        value.insertData(value_blob.data(), value_blob.size());
    }
}

}
