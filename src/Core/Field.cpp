#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/readDecimalText.h>
#include <Core/Field.h>
#include <Core/DecimalComparison.h>
#include <Common/FieldVisitors.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_FROM_FIELD_DUMP;
    extern const int DECIMAL_OVERFLOW;
}

void readBinary(Array & x, ReadBuffer & buf)
{
    size_t size;
    UInt8 type;
    DB::readBinary(type, buf);
    DB::readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        switch (type)
        {
            case Field::Types::Null:
            {
                x.push_back(DB::Field());
                break;
            }
            case Field::Types::UInt64:
            {
                UInt64 value;
                DB::readVarUInt(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::UInt128:
            {
                UInt128 value;
                DB::readBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::Int64:
            {
                Int64 value;
                DB::readVarInt(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::Float64:
            {
                Float64 value;
                DB::readFloatBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::String:
            {
                std::string value;
                DB::readStringBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::Array:
            {
                Array value;
                DB::readBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::Tuple:
            {
                Tuple value;
                DB::readBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::AggregateFunctionState:
            {
                AggregateFunctionStateData value;
                DB::readStringBinary(value.name, buf);
                DB::readStringBinary(value.data, buf);
                x.push_back(value);
                break;
            }
        }
    }
}

void writeBinary(const Array & x, WriteBuffer & buf)
{
    UInt8 type = Field::Types::Null;
    size_t size = x.size();
    if (size)
        type = x.front().getType();
    DB::writeBinary(type, buf);
    DB::writeBinary(size, buf);

    for (const auto & elem : x)
    {
        switch (type)
        {
            case Field::Types::Null: break;
            case Field::Types::UInt64:
            {
                DB::writeVarUInt(get<UInt64>(elem), buf);
                break;
            }
            case Field::Types::UInt128:
            {
                DB::writeBinary(get<UInt128>(elem), buf);
                break;
            }
            case Field::Types::Int64:
            {
                DB::writeVarInt(get<Int64>(elem), buf);
                break;
            }
            case Field::Types::Float64:
            {
                DB::writeFloatBinary(get<Float64>(elem), buf);
                break;
            }
            case Field::Types::String:
            {
                DB::writeStringBinary(get<std::string>(elem), buf);
                break;
            }
            case Field::Types::Array:
            {
                DB::writeBinary(get<Array>(elem), buf);
                break;
            }
            case Field::Types::Tuple:
            {
                DB::writeBinary(get<Tuple>(elem), buf);
                break;
            }
            case Field::Types::AggregateFunctionState:
            {
                DB::writeStringBinary(elem.get<AggregateFunctionStateData>().name, buf);
                DB::writeStringBinary(elem.get<AggregateFunctionStateData>().data, buf);
                break;
            }
        }
    }
}

void writeText(const Array & x, WriteBuffer & buf)
{
    DB::String res = applyVisitor(DB::FieldVisitorToString(), DB::Field(x));
    buf.write(res.data(), res.size());
}

void readBinary(Tuple & x, ReadBuffer & buf)
{
    size_t size;
    DB::readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        DB::readBinary(type, buf);

        switch (type)
        {
            case Field::Types::Null:
            {
                x.push_back(DB::Field());
                break;
            }
            case Field::Types::UInt64:
            {
                UInt64 value;
                DB::readVarUInt(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::UInt128:
            {
                UInt128 value;
                DB::readBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::Int64:
            {
                Int64 value;
                DB::readVarInt(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::Float64:
            {
                Float64 value;
                DB::readFloatBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::String:
            {
                std::string value;
                DB::readStringBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::Array:
            {
                Array value;
                DB::readBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::Tuple:
            {
                Tuple value;
                DB::readBinary(value, buf);
                x.push_back(value);
                break;
            }
            case Field::Types::AggregateFunctionState:
            {
                AggregateFunctionStateData value;
                DB::readStringBinary(value.name, buf);
                DB::readStringBinary(value.data, buf);
                x.push_back(value);
                break;
            }
        }
    }
}

void writeBinary(const Tuple & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    DB::writeBinary(size, buf);

    for (const auto & elem : x)
    {
        const UInt8 type = elem.getType();
        DB::writeBinary(type, buf);

        switch (type)
        {
            case Field::Types::Null: break;
            case Field::Types::UInt64:
            {
                DB::writeVarUInt(get<UInt64>(elem), buf);
                break;
            }
            case Field::Types::UInt128:
            {
                DB::writeBinary(get<UInt128>(elem), buf);
                break;
            }
            case Field::Types::Int64:
            {
                DB::writeVarInt(get<Int64>(elem), buf);
                break;
            }
            case Field::Types::Float64:
            {
                DB::writeFloatBinary(get<Float64>(elem), buf);
                break;
            }
            case Field::Types::String:
            {
                DB::writeStringBinary(get<std::string>(elem), buf);
                break;
            }
            case Field::Types::Array:
            {
                DB::writeBinary(get<Array>(elem), buf);
                break;
            }
            case Field::Types::Tuple:
            {
                DB::writeBinary(get<Tuple>(elem), buf);
                break;
            }
            case Field::Types::AggregateFunctionState:
            {
                DB::writeStringBinary(elem.get<AggregateFunctionStateData>().name, buf);
                DB::writeStringBinary(elem.get<AggregateFunctionStateData>().data, buf);
                break;
            }
        }
    }
}

void writeText(const Tuple & x, WriteBuffer & buf)
{
    writeFieldText(DB::Field(x), buf);
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
        if (common::mulOverflow(value.value, T::getScaleMultiplier(exponent), value.value))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }
    else
        scale = -exponent;
    assertChar('\'', buf);
    x = DecimalField<T>{value, scale};
}

template void readQuoted<Decimal32>(DecimalField<Decimal32> & x, ReadBuffer & buf);
template void readQuoted<Decimal64>(DecimalField<Decimal64> & x, ReadBuffer & buf);
template void readQuoted<Decimal128>(DecimalField<Decimal128> & x, ReadBuffer & buf);


void writeFieldText(const Field & x, WriteBuffer & buf)
{
    DB::String res = Field::dispatch(DB::FieldVisitorToString(), x);
    buf.write(res.data(), res.size());
}


String Field::dump() const
{
    return applyVisitor(DB::FieldVisitorDump(), *this);
}

Field Field::restoreFromDump(const std::string_view & dump_)
{
    auto show_error = [&dump_]
    {
        throw Exception("Couldn't restore Field from dump: " + String{dump_}, ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP);
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

    prefix = std::string_view{"UUID_"};
    if (dump.starts_with(prefix))
    {
        UUID uuid;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(uuid, buf);
        return uuid;
    }

    if (dump.starts_with("\'"))
    {
        String str;
        ReadBufferFromString buf{dump};
        readQuoted(str, buf);
        return str;
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
    __builtin_unreachable();
}


template <typename T>
static bool decEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
static bool decLess(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
static bool decLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <> bool decimalEqual(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale) { return decEqual(x, y, x_scale, y_scale); }
template <> bool decimalLess(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale) { return decLess(x, y, x_scale, y_scale); }
template <> bool decimalLessOrEqual(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale) { return decLessOrEqual(x, y, x_scale, y_scale); }

template <> bool decimalEqual(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale) { return decEqual(x, y, x_scale, y_scale); }
template <> bool decimalLess(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale) { return decLess(x, y, x_scale, y_scale); }
template <> bool decimalLessOrEqual(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale) { return decLessOrEqual(x, y, x_scale, y_scale); }

template <> bool decimalEqual(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale) { return decEqual(x, y, x_scale, y_scale); }
template <> bool decimalLess(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale) { return decLess(x, y, x_scale, y_scale); }
template <> bool decimalLessOrEqual(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale) { return decLessOrEqual(x, y, x_scale, y_scale); }

}
