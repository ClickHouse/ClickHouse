#include <Common/FieldVisitorWriteBinary.h>
#include <Common/checkStackSize.h>

#include <IO/WriteHelpers.h>

#include <string_view>


namespace DB
{

namespace
{

size_t getStringBinarySize(std::string_view str)
{
    return getLengthOfVarUInt(str.size()) + str.size();
}

size_t getStringBinarySize(const String & str)
{
    return getLengthOfVarUInt(str.size()) + str.size();
}

size_t getStringBinarySize(const char * str)
{
    return getStringBinarySize(std::string_view(str));
}

size_t getFieldBinaryPayloadSize(const Field & x);

template <typename Container>
size_t getIndexedContainerBinarySize(const Container & x)
{
    checkStackSize();

    size_t result = sizeof(size_t);
    const size_t size = x.size();
    for (size_t i = 0; i < size; ++i)
        result += sizeof(UInt8) + getFieldBinaryPayloadSize(x[i]);

    return result;
}

class FieldVisitorGetBinarySize
{
public:
    size_t operator() (const Null &) const { return 0; }
    size_t operator() (const UInt64 & x) const { return getLengthOfVarUInt(x); }
    size_t operator() (const Int64 & x) const { return getLengthOfVarInt(x); }
    size_t operator() (const Float64 &) const { return sizeof(Float64); }
    size_t operator() (const String & x) const { return getStringBinarySize(x); }
    size_t operator() (const UInt128 &) const { return sizeof(UInt128); }
    size_t operator() (const Int128 &) const { return sizeof(Int128); }
    size_t operator() (const UInt256 &) const { return sizeof(UInt256); }
    size_t operator() (const Int256 &) const { return sizeof(Int256); }
    size_t operator() (const UUID &) const { return sizeof(UUID); }
    size_t operator() (const IPv4 &) const { return sizeof(IPv4); }
    size_t operator() (const IPv6 &) const { return sizeof(IPv6); }
    size_t operator() (const CustomType & x) const { return getStringBinarySize(x.getTypeName()) + getStringBinarySize(x.toString()); }
    size_t operator() (const DecimalField<Decimal32> &) const { return sizeof(Decimal32) + sizeof(UInt32); }
    size_t operator() (const DecimalField<Decimal64> &) const { return sizeof(Decimal64) + sizeof(UInt32); }
    size_t operator() (const DecimalField<Decimal128> &) const { return sizeof(Decimal128) + sizeof(UInt32); }
    size_t operator() (const DecimalField<Decimal256> &) const { return sizeof(Decimal256) + sizeof(UInt32); }
    size_t operator() (const AggregateFunctionStateData & x) const { return getStringBinarySize(x.name) + getStringBinarySize(x.data); }
    size_t operator() (const Array & x) const { return getIndexedContainerBinarySize(x); }
    size_t operator() (const Tuple & x) const { return getIndexedContainerBinarySize(x); }
    size_t operator() (const Map & x) const { return getIndexedContainerBinarySize(x); }

    size_t operator() (const Object & x) const
    {
        checkStackSize();

        size_t result = sizeof(size_t);
        for (const auto & [key, value] : x)
            result += sizeof(UInt8) + getStringBinarySize(key) + getFieldBinaryPayloadSize(value);

        return result;
    }

    size_t operator() (const bool &) const { return sizeof(UInt8); }
};

size_t getFieldBinaryPayloadSize(const Field & x)
{
    return Field::dispatch(FieldVisitorGetBinarySize(), x);
}

}

void FieldVisitorWriteBinary::operator() (const Null &, WriteBuffer &) const {}
void FieldVisitorWriteBinary::operator() (const UInt64 & x, WriteBuffer & buf) const { writeVarUInt(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int64 & x, WriteBuffer & buf) const { writeVarInt(x, buf); }
void FieldVisitorWriteBinary::operator() (const Float64 & x, WriteBuffer & buf) const { writeFloatBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const String & x, WriteBuffer & buf) const { writeStringBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const UInt128 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int128 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const UInt256 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int256 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const UUID & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const IPv4 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const IPv6 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const CustomType & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal32> & x, WriteBuffer & buf) const { writeBinary(x.getValue(), buf); writeBinary(x.getScale(), buf); }
void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal64> & x, WriteBuffer & buf) const { writeBinary(x.getValue(), buf); writeBinary(x.getScale(), buf); }
void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal128> & x, WriteBuffer & buf) const { writeBinary(x.getValue(), buf); writeBinary(x.getScale(), buf); }
void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal256> & x, WriteBuffer & buf) const { writeBinary(x.getValue(), buf); writeBinary(x.getScale(), buf); }
void FieldVisitorWriteBinary::operator() (const AggregateFunctionStateData & x, WriteBuffer & buf) const
{
    writeStringBinary(x.name, buf);
    writeStringBinary(x.data, buf);
}

void FieldVisitorWriteBinary::operator() (const Array & x, WriteBuffer & buf) const
{
    checkStackSize();
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}

void FieldVisitorWriteBinary::operator() (const Tuple & x, WriteBuffer & buf) const
{
    checkStackSize();
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}


void FieldVisitorWriteBinary::operator() (const Map & x, WriteBuffer & buf) const
{
    checkStackSize();
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}

void FieldVisitorWriteBinary::operator() (const Object & x, WriteBuffer & buf) const
{
    checkStackSize();
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & [key, value] : x)
    {
        const UInt8 type = value.getType();
        writeBinary(type, buf);
        writeBinary(key, buf);
        Field::dispatch([&buf] (const auto & val) { FieldVisitorWriteBinary()(val, buf); }, value);
    }
}

void FieldVisitorWriteBinary::operator()(const bool & x, WriteBuffer & buf) const
{
    writeBinary(static_cast<UInt8>(x), buf);
}

size_t getFieldBinarySize(const Field & x)
{
    return sizeof(UInt8) + getFieldBinaryPayloadSize(x);
}

}
