#include <Common/Exception.h>
#include <Common/FieldVisitorDump.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorWriteBinary.h>
#include <Core/AccurateComparison.h>
#include <Core/DecimalComparison.h>
#include <Core/Field.h>
#include <Core/CompareHelper.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/readDecimalText.h>


using namespace std::literals;

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_TYPE_OF_FIELD;
extern const int BAD_GET;
extern const int CANNOT_RESTORE_FROM_FIELD_DUMP;
extern const int DECIMAL_OVERFLOW;
extern const int INCORRECT_DATA;
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

bool AggregateFunctionStateData::operator < (const AggregateFunctionStateData &) const
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Operator < is not implemented for AggregateFunctionStateData.");
}

bool AggregateFunctionStateData::operator <= (const AggregateFunctionStateData &) const
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Operator <= is not implemented for AggregateFunctionStateData.");
}

bool AggregateFunctionStateData::operator > (const AggregateFunctionStateData &) const
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Operator > is not implemented for AggregateFunctionStateData.");
}

bool AggregateFunctionStateData::operator >= (const AggregateFunctionStateData &) const
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Operator >= is not implemented for AggregateFunctionStateData.");
}

bool AggregateFunctionStateData::operator == (const AggregateFunctionStateData & rhs) const
{
    if (name != rhs.name)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Comparing aggregate functions with different types: {} and {}",
                name, rhs.name);

    return data == rhs.data;
}

template <is_decimal T>
T DecimalField<T>::getScaleMultiplier() const
{
    return DecimalUtils::scaleMultiplier<T>(scale);
}

template <is_decimal T>
const DecimalField<T> & DecimalField<T>::operator += (const DecimalField<T> & r)
{
    if (scale != r.getScale())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Add different decimal fields");
    dec += r.getValue();
    return *this;
}

template <is_decimal T>
const DecimalField<T> & DecimalField<T>::operator -= (const DecimalField<T> & r)
{
    if (scale != r.getScale())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sub different decimal fields");
    dec -= r.getValue();
    return *this;
}

bool Field::operator< (const Field & rhs) const
{
    if (which < rhs.which)
        return true;
    if (which > rhs.which)
        return false;

    switch (which)
    {
        case Types::Null:    return get<Null>() < rhs.get<Null>();
        case Types::Bool:    [[fallthrough]];
        case Types::UInt64:  return get<UInt64>()  < rhs.get<UInt64>();
        case Types::UInt128: return get<UInt128>() < rhs.get<UInt128>();
        case Types::UInt256: return get<UInt256>() < rhs.get<UInt256>();
        case Types::Int64:   return get<Int64>()   < rhs.get<Int64>();
        case Types::Int128:  return get<Int128>()  < rhs.get<Int128>();
        case Types::Int256:  return get<Int256>()  < rhs.get<Int256>();
        case Types::UUID:    return get<UUID>()    < rhs.get<UUID>();
        case Types::IPv4:    return get<IPv4>()    < rhs.get<IPv4>();
        case Types::IPv6:    return get<IPv6>()    < rhs.get<IPv6>();
        case Types::Float64:
            static constexpr int nan_direction_hint = 1; /// Put NaN at the end
            return FloatCompareHelper<Float64>::less(get<Float64>(), rhs.get<Float64>(), nan_direction_hint);
        case Types::String:  return get<String>()  < rhs.get<String>();
        case Types::Array:   return get<Array>()   < rhs.get<Array>();
        case Types::Tuple:   return get<Tuple>()   < rhs.get<Tuple>();
        case Types::Map:     return get<Map>()     < rhs.get<Map>();
        case Types::Object:  return get<Object>()  < rhs.get<Object>();
        case Types::Decimal32:  return get<DecimalField<Decimal32>>()  < rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:  return get<DecimalField<Decimal64>>()  < rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128: return get<DecimalField<Decimal128>>() < rhs.get<DecimalField<Decimal128>>();
        case Types::Decimal256: return get<DecimalField<Decimal256>>() < rhs.get<DecimalField<Decimal256>>();
        case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() < rhs.get<AggregateFunctionStateData>();
        case Types::CustomType:  return get<CustomType>() < rhs.get<CustomType>();
    }

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field");
}

bool Field::operator<= (const Field & rhs) const
{
    if (which < rhs.which)
        return true;
    if (which > rhs.which)
        return false;

    switch (which)
    {
        case Types::Null:    return get<Null>() <= rhs.get<Null>();
        case Types::Bool: [[fallthrough]];
        case Types::UInt64:  return get<UInt64>()  <= rhs.get<UInt64>();
        case Types::UInt128: return get<UInt128>() <= rhs.get<UInt128>();
        case Types::UInt256: return get<UInt256>() <= rhs.get<UInt256>();
        case Types::Int64:   return get<Int64>()   <= rhs.get<Int64>();
        case Types::Int128:  return get<Int128>()  <= rhs.get<Int128>();
        case Types::Int256:  return get<Int256>()  <= rhs.get<Int256>();
        case Types::UUID:    return get<UUID>().toUnderType() <= rhs.get<UUID>().toUnderType();
        case Types::IPv4:    return get<IPv4>()    <= rhs.get<IPv4>();
        case Types::IPv6:    return get<IPv6>()    <= rhs.get<IPv6>();
        case Types::Float64:
        {
            static constexpr int nan_direction_hint = 1; /// Put NaN at the end
            Float64 f1 = get<Float64>();
            Float64 f2 = get<Float64>();
            return FloatCompareHelper<Float64>::less(f1, f2, nan_direction_hint)
                || FloatCompareHelper<Float64>::equals(f1, f2, nan_direction_hint);
        }
        case Types::String:  return get<String>()  <= rhs.get<String>();
        case Types::Array:   return get<Array>()   <= rhs.get<Array>();
        case Types::Tuple:   return get<Tuple>()   <= rhs.get<Tuple>();
        case Types::Map:     return get<Map>()     <= rhs.get<Map>();
        case Types::Object:  return get<Object>()  <= rhs.get<Object>();
        case Types::Decimal32:  return get<DecimalField<Decimal32>>()  <= rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:  return get<DecimalField<Decimal64>>()  <= rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128: return get<DecimalField<Decimal128>>() <= rhs.get<DecimalField<Decimal128>>();
        case Types::Decimal256: return get<DecimalField<Decimal256>>() <= rhs.get<DecimalField<Decimal256>>();
        case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() <= rhs.get<AggregateFunctionStateData>();
        case Types::CustomType:  return get<CustomType>() <= rhs.get<CustomType>();
    }

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field");
}

bool Field::operator== (const Field & rhs) const
{
    if (which != rhs.which)
        return false;

    switch (which)
    {
        case Types::Null: return get<Null>() == rhs.get<Null>();
        case Types::Bool: [[fallthrough]];
        case Types::UInt64: return get<UInt64>() == rhs.get<UInt64>();
        case Types::Int64:   return get<Int64>() == rhs.get<Int64>();
        case Types::Float64:
            static constexpr int nan_direction_hint = 1; /// Put NaN at the end
            return FloatCompareHelper<Float64>::equals(get<Float64>(), rhs.get<Float64>(), nan_direction_hint);
        case Types::UUID:    return get<UUID>()    == rhs.get<UUID>();
        case Types::IPv4:    return get<IPv4>()    == rhs.get<IPv4>();
        case Types::IPv6:    return get<IPv6>()    == rhs.get<IPv6>();
        case Types::String:  return get<String>()  == rhs.get<String>();
        case Types::Array:   return get<Array>()   == rhs.get<Array>();
        case Types::Tuple:   return get<Tuple>()   == rhs.get<Tuple>();
        case Types::Map:     return get<Map>()     == rhs.get<Map>();
        case Types::Object:  return get<Object>()  == rhs.get<Object>();
        case Types::UInt128: return get<UInt128>() == rhs.get<UInt128>();
        case Types::UInt256: return get<UInt256>() == rhs.get<UInt256>();
        case Types::Int128:  return get<Int128>()  == rhs.get<Int128>();
        case Types::Int256:  return get<Int256>()  == rhs.get<Int256>();
        case Types::Decimal32:  return get<DecimalField<Decimal32>>()  == rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:  return get<DecimalField<Decimal64>>()  == rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128: return get<DecimalField<Decimal128>>() == rhs.get<DecimalField<Decimal128>>();
        case Types::Decimal256: return get<DecimalField<Decimal256>>() == rhs.get<DecimalField<Decimal256>>();
        case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() == rhs.get<AggregateFunctionStateData>();
        case Types::CustomType:  return get<CustomType>() == rhs.get<CustomType>();
    }

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field");
}

Field getBinaryValue(UInt8 type, ReadBuffer & buf)
{
    switch (static_cast<Field::Types::Which>(type))
    {
        case Field::Types::Null:
        {
            return Field();
        }
        case Field::Types::UInt64:
        {
            UInt64 value;
            readVarUInt(value, buf);
            return value;
        }
        case Field::Types::UInt128:
        {
            UInt128 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::UInt256:
        {
            UInt256 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::UUID:
        {
            UUID value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::IPv4:
        {
            IPv4 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::IPv6:
        {
            IPv6 value;
            readBinary(value.toUnderType(), buf);
            return value;
        }
        case Field::Types::Int64:
        {
            Int64 value;
            readVarInt(value, buf);
            return value;
        }
        case Field::Types::Int128:
        {
            Int128 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Int256:
        {
            Int256 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Float64:
        {
            Float64 value;
            readFloatBinary(value, buf);
            return value;
        }
        case Field::Types::String:
        {
            std::string value;
            readStringBinary(value, buf);
            return value;
        }
        case Field::Types::Array:
        {
            Array value;
            readBinaryArray(value, buf);
            return value;
        }
        case Field::Types::Tuple:
        {
            Tuple value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Map:
        {
            Map value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Object:
        {
            Object value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::AggregateFunctionState:
        {
            AggregateFunctionStateData value;
            readStringBinary(value.name, buf);
            readStringBinary(value.data, buf);
            return value;
        }
        case Field::Types::Bool:
        {
            UInt8 value;
            readBinary(value, buf);
            return bool(value);
        }
        case Field::Types::Decimal32:
        {
            Decimal<Int32> value;
            readBinary(value, buf);
            UInt32 scale = 0 ;
            readBinary(scale, buf);
            return DecimalField<Decimal32>(value, scale);
        }
        case Field::Types::Decimal64:
        {
            Decimal<Int64> value;
            readBinary(value, buf);
            UInt32 scale = 0;
            readBinary(scale, buf);
            return DecimalField<Decimal64>(value, scale);
        }
        case Field::Types::Decimal128:
        {
            Decimal<Int128> value;
            readBinary(value, buf);
            UInt32 scale = 0;
            readBinary(scale, buf);
            return DecimalField<Decimal128>(value, scale);
        }
        case Field::Types::Decimal256:
        {
            Decimal<Int256> value;
            readBinary(value, buf);
            UInt32 scale = 0;
            readBinary(scale, buf);
            return DecimalField<Decimal256>(value, scale);
        }
        case Field::Types::CustomType:
            return Field();
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field type {}", std::to_string(type));
}

void readBinaryArray(Array & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
        x.push_back(readFieldBinary(buf));
}

void writeBinaryArray(const Array & x, WriteBuffer & buf)
{
    size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & elem : x)
        writeFieldBinary(elem, buf);
}

void writeText(const Array & x, WriteBuffer & buf)
{
    String res = applyVisitor(FieldVisitorToString(), Field(x));
    buf.write(res.data(), res.size());
}

void readBinary(Tuple & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
        x.push_back(readFieldBinary(buf));
}

void writeBinary(const Tuple & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & elem : x)
        writeFieldBinary(elem, buf);
}

void writeText(const Tuple & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

void readBinary(Map & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
        x.push_back(readFieldBinary(buf));
}

void writeBinary(const Map & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & elem : x)
        writeFieldBinary(elem, buf);
}

void writeText(const Map & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

void readBinary(Object & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        String key;
        readBinary(type, buf);
        readBinary(key, buf);
        x[key] = getBinaryValue(type, buf);
    }
}

void writeBinary(const Object & x, WriteBuffer & buf)
{
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

void writeText(const Object & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

void writeBinary(const CustomType & x, WriteBuffer & buf)
{
    writeBinary(std::string_view(x.getTypeName()), buf);
    writeBinary(x.toString(), buf);
}

void writeText(const CustomType & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

template <typename T>
void readQuoted(DecimalField<T> & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    T value;
    UInt32 scale;
    int32_t exponent;
    uint32_t max_digits = static_cast<uint32_t>(-1);
    readDigits<true>(buf, value, max_digits, exponent, true);
    if (exponent > 0)
    {
        scale = 0;
        if (common::mulOverflow(value.value, DecimalUtils::scaleMultiplier<T>(exponent), value.value))
            throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
    }
    else
        scale = -exponent;
    assertChar('\'', buf);
    x = DecimalField<T>{value, scale};
}

[[noreturn]] void readText(Array &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Array."); }
[[noreturn]] void readQuoted(Array &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Array."); }
[[noreturn]] void writeQuoted(const Array &, WriteBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write Array quoted."); }
[[noreturn]] void readText(Tuple &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Tuple."); }
[[noreturn]] void readQuoted(Tuple &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Tuple."); }
[[noreturn]] void writeQuoted(const Tuple &, WriteBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write Tuple quoted."); }
[[noreturn]] void readText(Map &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Map."); }
[[noreturn]] void readQuoted(Map &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Map."); }
[[noreturn]] void writeQuoted(const Map &, WriteBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write Map quoted."); }
[[noreturn]] void readText(Object &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Object."); }
[[noreturn]] void readQuoted(Object &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Object."); }
[[noreturn]] void writeQuoted(const Object &, WriteBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write Object quoted."); }
[[noreturn]] void writeText(const AggregateFunctionStateData &, WriteBuffer &)
{
    // This probably doesn't make any sense, but we have to have it for
    // completeness, so that we can use toString(field_value) in field visitors.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert a Field of type AggregateFunctionStateData to human-readable text");
}


template void readQuoted<Decimal32>(DecimalField<Decimal32> & x, ReadBuffer & buf);
template void readQuoted<Decimal64>(DecimalField<Decimal64> & x, ReadBuffer & buf);
template void readQuoted<Decimal128>(DecimalField<Decimal128> & x, ReadBuffer & buf);
template void readQuoted<Decimal256>(DecimalField<Decimal256> & x, ReadBuffer & buf);

void writeFieldText(const Field & x, WriteBuffer & buf)
{
    String res = Field::dispatch(FieldVisitorToString(), x);
    buf.write(res.data(), res.size());
}

void writeFieldBinary(const Field & x, WriteBuffer & buf)
{
    const UInt8 type = x.getType();
    writeBinary(type, buf);
    Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, x);
}

Field readFieldBinary(ReadBuffer & buf)
{
    UInt8 type;
    readBinary(type, buf);
    return getBinaryValue(type, buf);
}

String Field::dump() const
{
    return applyVisitor(FieldVisitorDump(), *this);
}

Field Field::restoreFromDump(std::string_view dump_)
{
    auto show_error = [&dump_]
    {
        throw Exception(ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP, "Couldn't restore Field from dump: {}", String{dump_});
    };

    std::string_view dump = dump_;
    trim(dump);

    if (dump == "NULL")
        return {};

    std::string_view prefix = std::string_view{"Int64_"};
    if (dump.starts_with(prefix))
    {
        Int64 value = parseFromString<Int64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UInt64_"};
    if (dump.starts_with(prefix))
    {
        UInt64 value = parseFromString<UInt64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Int128_"};
    if (dump.starts_with(prefix))
    {
        Int128 value = parseFromString<Int128>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UInt128_"};
    if (dump.starts_with(prefix))
    {
        UInt128 value = parseFromString<UInt128>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Int256_"};
    if (dump.starts_with(prefix))
    {
        Int256 value = parseFromString<Int256>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UInt256_"};
    if (dump.starts_with(prefix))
    {
        UInt256 value = parseFromString<UInt256>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Float64_"};
    if (dump.starts_with(prefix))
    {
        Float64 value = parseFromString<Float64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Decimal32_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal32> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"Decimal64_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal64> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"Decimal128_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal128> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"Decimal256_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal256> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    if (dump.starts_with("\'"))
    {
        String str;
        ReadBufferFromString buf{dump};
        readQuoted(str, buf);
        return str;
    }

    prefix = std::string_view{"Bool_"};
    if (dump.starts_with(prefix))
    {
        bool value = parseFromString<bool>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Array_["};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Array array;
        while (tail != "]")
        {
            size_t separator = tail.find_first_of(",]");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != "]")
                show_error();
            array.push_back(Field::restoreFromDump(element));
        }
        return array;
    }

    prefix = std::string_view{"Tuple_("};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Tuple tuple;
        while (tail != ")")
        {
            size_t separator = tail.find_first_of(",)");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != ")")
                show_error();
            tuple.push_back(Field::restoreFromDump(element));
        }
        return tuple;
    }

    prefix = std::string_view{"Map_("};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Map map;
        while (tail != ")")
        {
            size_t separator = tail.find_first_of(",)");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != ")")
                show_error();
            map.push_back(Field::restoreFromDump(element));
        }
        return map;
    }

    prefix = std::string_view{"AggregateFunctionState_("};
    if (dump.starts_with(prefix))
    {
        std::string_view after_prefix = dump.substr(prefix.length());
        size_t comma = after_prefix.find(',');
        size_t end = after_prefix.find(')', comma + 1);
        if ((comma == std::string_view::npos) || (end != after_prefix.length() - 1))
            show_error();
        std::string_view name_view = after_prefix.substr(0, comma);
        std::string_view data_view = after_prefix.substr(comma + 1, end - comma - 1);
        trim(name_view);
        trim(data_view);
        ReadBufferFromString name_buf{name_view};
        ReadBufferFromString data_buf{data_view};
        AggregateFunctionStateData res;
        readQuotedString(res.name, name_buf);
        readQuotedString(res.data, data_buf);
        return res;
    }

    show_error();
    UNREACHABLE();
}


template <typename T>
bool decimalEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    bool check_overflow = true;
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale, check_overflow);
}

template <typename T>
bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    bool check_overflow = true;
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale, check_overflow);
}

template <typename T>
bool decimalLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    bool check_overflow = true;
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale, check_overflow);
}


template bool decimalEqual<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Time64>(Time64 x, Time64 y, UInt32 x_scale, UInt32 y_scale);

template bool decimalLess<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Time64>(Time64 x, Time64 y, UInt32 x_scale, UInt32 y_scale);

template bool decimalLessOrEqual<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Time64>(Time64 x, Time64 y, UInt32 x_scale, UInt32 y_scale);


void writeText(const Null & x, WriteBuffer & buf)
{
    if (x.isNegativeInfinity())
        writeText("-Inf", buf);
    if (x.isPositiveInfinity())
        writeText("+Inf", buf);
    else
        writeText("NULL", buf);
}

String toString(const Field & x)
{
    return Field::dispatch(
        [] (const auto & value)
        {
            // Use explicit type to prevent implicit construction of Field and
            // infinite recursion into toString<Field>.
            return toString<decltype(value)>(value);
        },
        x);
}

std::string_view fieldTypeToString(Field::Types::Which type)
{
    switch (type)
    {
        case Field::Types::Which::Null: return "Null"sv;
        case Field::Types::Which::Array: return "Array"sv;
        case Field::Types::Which::Tuple: return "Tuple"sv;
        case Field::Types::Which::Map: return "Map"sv;
        case Field::Types::Which::Object: return "Object"sv;
        case Field::Types::Which::AggregateFunctionState: return "AggregateFunctionState"sv;
        case Field::Types::Which::Bool: return "Bool"sv;
        case Field::Types::Which::String: return "String"sv;
        case Field::Types::Which::Decimal32: return "Decimal32"sv;
        case Field::Types::Which::Decimal64: return "Decimal64"sv;
        case Field::Types::Which::Decimal128: return "Decimal128"sv;
        case Field::Types::Which::Decimal256: return "Decimal256"sv;
        case Field::Types::Which::Float64: return "Float64"sv;
        case Field::Types::Which::Int64: return "Int64"sv;
        case Field::Types::Which::Int128: return "Int128"sv;
        case Field::Types::Which::Int256: return "Int256"sv;
        case Field::Types::Which::UInt64: return "UInt64"sv;
        case Field::Types::Which::UInt128: return "UInt128"sv;
        case Field::Types::Which::UInt256: return "UInt256"sv;
        case Field::Types::Which::UUID: return "UUID"sv;
        case Field::Types::Which::IPv4: return "IPv4"sv;
        case Field::Types::Which::IPv6: return "IPv6"sv;
        case Field::Types::Which::CustomType: return "CustomType"sv;
    }
}

/// Keep in mind, that "magic_enum" is very expensive for compiler, that's why we don't use it.
std::string_view Field::getTypeName() const
{
    return fieldTypeToString(which);
}

template class DecimalField<Decimal32>;
template class DecimalField<Decimal64>;
template class DecimalField<Decimal128>;
template class DecimalField<Decimal256>;
template class DecimalField<DateTime64>;
template class DecimalField<Time64>;

template <typename T>
NearestFieldType<std::decay_t<T>> & Field::safeGet() &
{
    const Types::Which target = TypeToEnum<NearestFieldType<std::decay_t<T>>>::value;

    /// bool is stored as uint64, will be returned as UInt64 when requested as bool or UInt64, as Int64 when requested as Int64
    /// also allow UInt64 <-> Int64 conversion
    if (target != which &&
        !(which == Field::Types::Bool && (target == Field::Types::UInt64 || target == Field::Types::Int64)) &&
        !(isInt64OrUInt64FieldType(which) && isInt64OrUInt64FieldType(target)))
        throw Exception(ErrorCodes::BAD_GET, "Bad get: has {}, requested {}", getTypeName(), fieldTypeToString(target));

    return get<T>();
}

template NearestFieldType<std::decay_t<bool>> & Field::safeGet<bool>() &;
template NearestFieldType<std::decay_t<UInt8>> & Field::safeGet<UInt8>() &;
template NearestFieldType<std::decay_t<UInt16>> & Field::safeGet<UInt16>() &;
template NearestFieldType<std::decay_t<UInt32>> & Field::safeGet<UInt32>() &;
template NearestFieldType<std::decay_t<UInt64>> & Field::safeGet<UInt64>() &;
template NearestFieldType<std::decay_t<UInt128>> & Field::safeGet<UInt128>() &;
template NearestFieldType<std::decay_t<UInt256>> & Field::safeGet<UInt256>() &;
template NearestFieldType<std::decay_t<Int8>> & Field::safeGet<Int8>() &;
template NearestFieldType<std::decay_t<Int16>> & Field::safeGet<Int16>() &;
template NearestFieldType<std::decay_t<Int32>> & Field::safeGet<Int32>() &;
template NearestFieldType<std::decay_t<Int64>> & Field::safeGet<Int64>() &;
template NearestFieldType<std::decay_t<Int128>> & Field::safeGet<Int128>() &;
template NearestFieldType<std::decay_t<Int256>> & Field::safeGet<Int256>() &;
template NearestFieldType<std::decay_t<BFloat16>> & Field::safeGet<BFloat16>() &;
template NearestFieldType<std::decay_t<Float32>> & Field::safeGet<Float32>() &;
template NearestFieldType<std::decay_t<Float64>> & Field::safeGet<Float64>() &;
template NearestFieldType<std::decay_t<String>> & Field::safeGet<String>() &;
template NearestFieldType<std::decay_t<UUID>> & Field::safeGet<UUID>() &;
template NearestFieldType<std::decay_t<IPv4>> & Field::safeGet<IPv4>() &;
template NearestFieldType<std::decay_t<IPv6>> & Field::safeGet<IPv6>() &;
template NearestFieldType<std::decay_t<Decimal32>> & Field::safeGet<Decimal32>() &;
template NearestFieldType<std::decay_t<Decimal64>> & Field::safeGet<Decimal64>() &;
template NearestFieldType<std::decay_t<Decimal128>> & Field::safeGet<Decimal128>() &;
template NearestFieldType<std::decay_t<Decimal256>> & Field::safeGet<Decimal256>() &;
template NearestFieldType<std::decay_t<DateTime64>> & Field::safeGet<DateTime64>() &;
template NearestFieldType<std::decay_t<Time64>> & Field::safeGet<Time64>() &;
template NearestFieldType<std::decay_t<DecimalField<Decimal32>>> & Field::safeGet<DecimalField<Decimal32>>() &;
template NearestFieldType<std::decay_t<DecimalField<Decimal64>>> & Field::safeGet<DecimalField<Decimal64>>() &;
template NearestFieldType<std::decay_t<DecimalField<Decimal128>>> & Field::safeGet<DecimalField<Decimal128>>() &;
template NearestFieldType<std::decay_t<DecimalField<Decimal256>>> & Field::safeGet<DecimalField<Decimal256>>() &;
template NearestFieldType<std::decay_t<DecimalField<DateTime64>>> & Field::safeGet<DecimalField<DateTime64>>() &;
template NearestFieldType<std::decay_t<DecimalField<Time64>>> & Field::safeGet<DecimalField<Time64>>() &;
template NearestFieldType<std::decay_t<AggregateFunctionStateData>> & Field::safeGet<AggregateFunctionStateData>() &;
template NearestFieldType<std::decay_t<Array>> & Field::safeGet<Array>() &;
template NearestFieldType<std::decay_t<Map>> & Field::safeGet<Map>() &;
template NearestFieldType<std::decay_t<Object>> & Field::safeGet<Object>() &;
template NearestFieldType<std::decay_t<Tuple>> & Field::safeGet<Tuple>() &;
template NearestFieldType<std::decay_t<CustomType>> & Field::safeGet<CustomType>() &;
/// In Darwin unsigned long does not match any of the UInt* types
#ifdef OS_DARWIN
template NearestFieldType<std::decay_t<unsigned long>> & Field::safeGet<unsigned long>() &;
#endif
}
