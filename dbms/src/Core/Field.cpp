#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Core/Field.h>
#include <Core/DecimalComparison.h>
#include <Common/FieldVisitors.h>


namespace DB
{
    inline void readBinary(Array & x, ReadBuffer & buf)
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
            };
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

        for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
        {
            switch (type)
            {
                case Field::Types::Null: break;
                case Field::Types::UInt64:
                {
                    DB::writeVarUInt(get<UInt64>(*it), buf);
                    break;
                }
                case Field::Types::UInt128:
                {
                    DB::writeBinary(get<UInt128>(*it), buf);
                    break;
                }
                case Field::Types::Int64:
                {
                    DB::writeVarInt(get<Int64>(*it), buf);
                    break;
                }
                case Field::Types::Float64:
                {
                    DB::writeFloatBinary(get<Float64>(*it), buf);
                    break;
                }
                case Field::Types::String:
                {
                    DB::writeStringBinary(get<std::string>(*it), buf);
                    break;
                }
                case Field::Types::Array:
                {
                    DB::writeBinary(get<Array>(*it), buf);
                    break;
                }
                case Field::Types::Tuple:
                {
                    DB::writeBinary(get<Tuple>(*it), buf);
                    break;
                }
            };
        }
    }

    void writeText(const Array & x, WriteBuffer & buf)
    {
        DB::String res = applyVisitor(DB::FieldVisitorToString(), DB::Field(x));
        buf.write(res.data(), res.size());
    }
}


namespace DB
{
    inline void readBinary(Tuple & x_def, ReadBuffer & buf)
    {
        auto & x = x_def.toUnderType();
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
            };
        }
    }

    void writeBinary(const Tuple & x_def, WriteBuffer & buf)
    {
        auto & x = x_def.toUnderType();
        const size_t size = x.size();
        DB::writeBinary(size, buf);

        for (auto it = x.begin(); it != x.end(); ++it)
        {
            const UInt8 type = it->getType();
            DB::writeBinary(type, buf);

            switch (type)
            {
                case Field::Types::Null: break;
                case Field::Types::UInt64:
                {
                    DB::writeVarUInt(get<UInt64>(*it), buf);
                    break;
                }
                case Field::Types::UInt128:
                {
                    DB::writeBinary(get<UInt128>(*it), buf);
                    break;
                }
                case Field::Types::Int64:
                {
                    DB::writeVarInt(get<Int64>(*it), buf);
                    break;
                }
                case Field::Types::Float64:
                {
                    DB::writeFloatBinary(get<Float64>(*it), buf);
                    break;
                }
                case Field::Types::String:
                {
                    DB::writeStringBinary(get<std::string>(*it), buf);
                    break;
                }
                case Field::Types::Array:
                {
                    DB::writeBinary(get<Array>(*it), buf);
                    break;
                }
                case Field::Types::Tuple:
                {
                    DB::writeBinary(get<Tuple>(*it), buf);
                    break;
                }
            };
        }
    }

    void writeText(const Tuple & x, WriteBuffer & buf)
    {
        DB::String res = applyVisitor(DB::FieldVisitorToString(), DB::Field(x));
        buf.write(res.data(), res.size());
    }


    template <> Decimal32 DecimalField<Decimal32>::getScaleMultiplier() const
    {
        return DataTypeDecimal<Decimal32>::getScaleMultiplier(scale);
    }

    template <> Decimal64 DecimalField<Decimal64>::getScaleMultiplier() const
    {
        return DataTypeDecimal<Decimal64>::getScaleMultiplier(scale);
    }

    template <> Decimal128 DecimalField<Decimal128>::getScaleMultiplier() const
    {
        return DataTypeDecimal<Decimal128>::getScaleMultiplier(scale);
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

    template <> bool decimalEqual(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) { return decEqual(x, y, xs, ys); }
    template <> bool decimalLess(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) { return decLess(x, y, xs, ys); }
    template <> bool decimalLessOrEqual(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) { return decLessOrEqual(x, y, xs, ys); }

    template <> bool decimalEqual(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) { return decEqual(x, y, xs, ys); }
    template <> bool decimalLess(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) { return decLess(x, y, xs, ys); }
    template <> bool decimalLessOrEqual(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) { return decLessOrEqual(x, y, xs, ys); }

    template <> bool decimalEqual(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) { return decEqual(x, y, xs, ys); }
    template <> bool decimalLess(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) { return decLess(x, y, xs, ys); }
    template <> bool decimalLessOrEqual(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) { return decLessOrEqual(x, y, xs, ys); }
}
