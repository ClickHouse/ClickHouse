#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Functions/FunctionsComparison.h>


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


    bool DecimalField::operator < (const DecimalField & r) const
    {
        using Comparator = DecimalComparison<Decimal128, Decimal128, LessOp>;
        return Comparator::compare(Decimal128(dec), Decimal128(r.dec), scale, r.scale);
    }

    bool DecimalField::operator <= (const DecimalField & r) const
    {
        using Comparator = DecimalComparison<Decimal128, Decimal128, LessOrEqualsOp>;
        return Comparator::compare(Decimal128(dec), Decimal128(r.dec), scale, r.scale);
    }

    bool DecimalField::operator == (const DecimalField & r) const
    {
        using Comparator = DecimalComparison<Decimal128, Decimal128, EqualsOp>;
        return Comparator::compare(Decimal128(dec), Decimal128(r.dec), scale, r.scale);
    }
}
