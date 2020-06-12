#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Core/Field.h>
#include <Core/DecimalComparison.h>
#include <Common/FieldVisitors.h>


namespace DB
{
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

    void writeFieldText(const Field & x, WriteBuffer & buf)
    {
        DB::String res = Field::dispatch(DB::FieldVisitorToString(), x);
        buf.write(res.data(), res.size());
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
