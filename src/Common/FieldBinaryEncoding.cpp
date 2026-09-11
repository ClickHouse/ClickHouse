#include <Common/FieldBinaryEncoding.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int INCORRECT_DATA;
}

namespace
{

enum class FieldBinaryTypeIndex: uint8_t
{
    Null = 0x00,
    UInt64 = 0x01,
    Int64 = 0x02,
    UInt128 = 0x03,
    Int128 = 0x04,
    UInt256 = 0x05,
    Int256 = 0x06,
    Float64 = 0x07,
    Decimal32 = 0x08,
    Decimal64 = 0x09,
    Decimal128 = 0x0A,
    Decimal256 = 0x0B,
    String = 0x0C,
    Array = 0x0D,
    Tuple = 0x0E,
    Map = 0x0F,
    IPv4 = 0x10,
    IPv6 = 0x11,
    UUID = 0x12,
    Bool = 0x13,
    Object = 0x14,
    AggregateFunctionState = 0x15,

    NegativeInfinity = 0xFE,
    PositiveInfinity = 0xFF,
};

class FieldVisitorEncodeBinary
{
public:
    void operator() (const Null & x, WriteBuffer & buf) const;
    void operator() (const UInt64 & x, WriteBuffer & buf) const;
    void operator() (const UInt128 & x, WriteBuffer & buf) const;
    void operator() (const UInt256 & x, WriteBuffer & buf) const;
    void operator() (const Int64 & x, WriteBuffer & buf) const;
    void operator() (const Int128 & x, WriteBuffer & buf) const;
    void operator() (const Int256 & x, WriteBuffer & buf) const;
    void operator() (const UUID & x, WriteBuffer & buf) const;
    void operator() (const IPv4 & x, WriteBuffer & buf) const;
    void operator() (const IPv6 & x, WriteBuffer & buf) const;
    void operator() (const Float64 & x, WriteBuffer & buf) const;
    void operator() (const String & x, WriteBuffer & buf) const;
    void operator() (const Array & x, WriteBuffer & buf) const;
    void operator() (const Tuple & x, WriteBuffer & buf) const;
    void operator() (const Map & x, WriteBuffer & buf) const;
    void operator() (const Object & x, WriteBuffer & buf) const;
    void operator() (const DecimalField<Decimal32> & x, WriteBuffer & buf) const;
    void operator() (const DecimalField<Decimal64> & x, WriteBuffer & buf) const;
    void operator() (const DecimalField<Decimal128> & x, WriteBuffer & buf) const;
    void operator() (const DecimalField<Decimal256> & x, WriteBuffer & buf) const;
    void operator() (const AggregateFunctionStateData & x, WriteBuffer & buf) const;
    [[noreturn]] void operator() (const CustomType & x, WriteBuffer & buf) const;
    void operator() (const bool & x, WriteBuffer & buf) const;
};

void FieldVisitorEncodeBinary::operator() (const Null & x, WriteBuffer & buf) const
{
    if (x.isNull())
        writeBinary(UInt8(FieldBinaryTypeIndex::Null), buf);
    else if (x.isPositiveInfinity())
        writeBinary(UInt8(FieldBinaryTypeIndex::PositiveInfinity), buf);
    else if (x.isNegativeInfinity())
        writeBinary(UInt8(FieldBinaryTypeIndex::NegativeInfinity), buf);
}

void FieldVisitorEncodeBinary::operator() (const UInt64 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::UInt64), buf);
    writeVarUInt(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const Int64 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Int64), buf);
    writeVarInt(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const Float64 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Float64), buf);
    writeBinaryLittleEndian(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const String & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::String), buf);
    writeStringBinary(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const UInt128 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::UInt128), buf);
    writeBinaryLittleEndian(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const Int128 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Int128), buf);
    writeBinaryLittleEndian(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const UInt256 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::UInt256), buf);
    writeBinaryLittleEndian(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const Int256 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Int256), buf);
    writeBinaryLittleEndian(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const UUID & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::UUID), buf);
    writeBinaryLittleEndian(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const IPv4 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::IPv4), buf);
    writeBinaryLittleEndian(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const IPv6 & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::IPv6), buf);
    writeBinaryLittleEndian(x, buf);
}

void FieldVisitorEncodeBinary::operator() (const DecimalField<Decimal32> & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Decimal32), buf);
    writeVarUInt(x.getScale(), buf);
    writeBinaryLittleEndian(x.getValue(), buf);
}

void FieldVisitorEncodeBinary::operator() (const DecimalField<Decimal64> & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Decimal64), buf);
    writeVarUInt(x.getScale(), buf);
    writeBinaryLittleEndian(x.getValue(), buf);
}

void FieldVisitorEncodeBinary::operator() (const DecimalField<Decimal128> & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Decimal128), buf);
    writeVarUInt(x.getScale(), buf);
    writeBinaryLittleEndian(x.getValue(), buf);
}

void FieldVisitorEncodeBinary::operator() (const DecimalField<Decimal256> & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Decimal256), buf);
    writeVarUInt(x.getScale(), buf);
    writeBinaryLittleEndian(x.getValue(), buf);
}

void FieldVisitorEncodeBinary::operator() (const AggregateFunctionStateData & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::AggregateFunctionState), buf);
    writeStringBinary(x.name, buf);
    writeStringBinary(x.data, buf);
}

void FieldVisitorEncodeBinary::operator() (const Array & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Array), buf);
    size_t size = x.size();
    writeVarUInt(size, buf);
    for (size_t i = 0; i < size; ++i)
        Field::dispatch([&buf] (const auto & value) { FieldVisitorEncodeBinary()(value, buf); }, x[i]);
}

void FieldVisitorEncodeBinary::operator() (const Tuple & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Tuple), buf);
    size_t size = x.size();
    writeVarUInt(size, buf);
    for (size_t i = 0; i < size; ++i)
        Field::dispatch([&buf] (const auto & value) { FieldVisitorEncodeBinary()(value, buf); }, x[i]);
}

void FieldVisitorEncodeBinary::operator() (const Map & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Map), buf);
    size_t size = x.size();
    writeVarUInt(size, buf);
    for (size_t i = 0; i < size; ++i)
    {
        const Tuple & key_and_value = x[i].safeGet<Tuple>();
        Field::dispatch([&buf] (const auto & value) { FieldVisitorEncodeBinary()(value, buf); }, key_and_value[0]);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorEncodeBinary()(value, buf); }, key_and_value[1]);
    }
}

void FieldVisitorEncodeBinary::operator() (const Object & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Object), buf);

    size_t size = x.size();
    writeVarUInt(size, buf);
    for (const auto & [key, value] : x)
    {
        writeStringBinary(key, buf);
        Field::dispatch([&buf] (const auto & val) { FieldVisitorEncodeBinary()(val, buf); }, value);
    }
}

void FieldVisitorEncodeBinary::operator()(const bool & x, WriteBuffer & buf) const
{
    writeBinary(UInt8(FieldBinaryTypeIndex::Bool), buf);
    writeBinary(static_cast<UInt8>(x), buf);
}

[[noreturn]] void FieldVisitorEncodeBinary::operator()(const CustomType &, WriteBuffer &) const
{
    /// TODO: Support binary encoding/decoding for custom types somehow.
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Binary encoding of Field with custom type is not supported");
}

template <typename T>
Field decodeBigInteger(ReadBuffer & buf)
{
    T value;
    readBinaryLittleEndian(value, buf);
    return value;
}

template <typename T>
DecimalField<T> decodeDecimal(ReadBuffer & buf)
{
    UInt32 scale;
    readVarUInt(scale, buf);
    T value;
    readBinaryLittleEndian(value, buf);
    return DecimalField<T>(value, scale);
}

template <typename T>
T decodeValueLittleEndian(ReadBuffer & buf)
{
    T value;
    readBinaryLittleEndian(value, buf);
    return value;
}

template <typename T>
T decodeArrayLikeField(ReadBuffer & buf)
{
    size_t size;
    readVarUInt(size, buf);
    T value;
    for (size_t i = 0; i != size; ++i)
        value.push_back(decodeField(buf));
    return value;
}

}
void encodeField(const Field & x, WriteBuffer & buf)
{
    Field::dispatch([&buf] (const auto & val) { FieldVisitorEncodeBinary()(val, buf); }, x);
}

Field decodeField(ReadBuffer & buf)
{
    UInt8 type;
    readBinary(type, buf);
    switch (FieldBinaryTypeIndex(type))
    {
        case FieldBinaryTypeIndex::Null:
            return Null();
        case FieldBinaryTypeIndex::PositiveInfinity:
            return POSITIVE_INFINITY;
        case FieldBinaryTypeIndex::NegativeInfinity:
            return NEGATIVE_INFINITY;
        case FieldBinaryTypeIndex::Int64:
        {
            Int64 value;
            readVarInt(value, buf);
            return value;
        }
        case FieldBinaryTypeIndex::UInt64:
        {
            UInt64 value;
            readVarUInt(value, buf);
            return value;
        }
        case FieldBinaryTypeIndex::Int128:
            return decodeBigInteger<Int128>(buf);
        case FieldBinaryTypeIndex::UInt128:
            return decodeBigInteger<UInt128>(buf);
        case FieldBinaryTypeIndex::Int256:
            return decodeBigInteger<Int256>(buf);
        case FieldBinaryTypeIndex::UInt256:
            return decodeBigInteger<UInt256>(buf);
        case FieldBinaryTypeIndex::Float64:
            return decodeValueLittleEndian<Float64>(buf);
        case FieldBinaryTypeIndex::Decimal32:
            return decodeDecimal<Decimal32>(buf);
        case FieldBinaryTypeIndex::Decimal64:
            return decodeDecimal<Decimal64>(buf);
        case FieldBinaryTypeIndex::Decimal128:
            return decodeDecimal<Decimal128>(buf);
        case FieldBinaryTypeIndex::Decimal256:
            return decodeDecimal<Decimal256>(buf);
        case FieldBinaryTypeIndex::String:
        {
            String value;
            readStringBinary(value, buf);
            return value;
        }
        case FieldBinaryTypeIndex::UUID:
            return decodeValueLittleEndian<UUID>(buf);
        case FieldBinaryTypeIndex::IPv4:
            return decodeValueLittleEndian<IPv4>(buf);
        case FieldBinaryTypeIndex::IPv6:
            return decodeValueLittleEndian<IPv6>(buf);
        case FieldBinaryTypeIndex::Bool:
        {
            bool value;
            readBinary(value, buf);
            return value;
        }
        case FieldBinaryTypeIndex::Array:
            return decodeArrayLikeField<Array>(buf);
        case FieldBinaryTypeIndex::Tuple:
            return decodeArrayLikeField<Tuple>(buf);
        case FieldBinaryTypeIndex::Map:
        {
            size_t size;
            readVarUInt(size, buf);
            Map map;
            for (size_t i = 0; i != size; ++i)
            {
                Tuple key_and_value;
                key_and_value.push_back(decodeField(buf));
                key_and_value.push_back(decodeField(buf));
                map.push_back(key_and_value);
            }
            return map;
        }
        case FieldBinaryTypeIndex::Object:
        {
            size_t size;
            readVarUInt(size, buf);
            Object value;
            for (size_t i = 0; i != size; ++i)
            {
                String name;
                readStringBinary(name, buf);
                value[name] = decodeField(buf);
            }
            return value;
        }
        case FieldBinaryTypeIndex::AggregateFunctionState:
        {
            String name;
            readStringBinary(name, buf);
            String data;
            readStringBinary(data, buf);
            return AggregateFunctionStateData{.name = name, .data = data};
        }
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown Field type: {0:#04x}", UInt64(type));
}

}
