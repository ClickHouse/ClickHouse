#include <Common/FieldVisitorHash.h>

#include <Common/NaNUtils.h>
#include <Common/SipHash.h>
#include <Core/DecimalFunctions.h>


namespace DB
{

FieldVisitorHash::FieldVisitorHash(SipHash & hash_) : hash(hash_) {}

void FieldVisitorHash::operator() (const Null &) const
{
    UInt8 type = Field::Types::Null;
    hash.update(type);
}

void FieldVisitorHash::operator() (const UInt64 & x) const
{
    UInt8 type = Field::Types::UInt64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const UInt128 & x) const
{
    UInt8 type = Field::Types::UInt128;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Int64 & x) const
{
    UInt8 type = Field::Types::Int64;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Int128 & x) const
{
    UInt8 type = Field::Types::Int128;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const UUID & x) const
{
    UInt8 type = Field::Types::UUID;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const IPv4 & x) const
{
    UInt8 type = Field::Types::IPv4;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const IPv6 & x) const
{
    UInt8 type = Field::Types::IPv6;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Float64 & x) const
{
    UInt8 type = Field::Types::Float64;
    hash.update(type);

    /// Normalize to be consistent with Field::operator== (FloatCompareHelper).
    if (isNaN(x))
    {
        static constexpr Float64 canonical_nan = std::numeric_limits<Float64>::quiet_NaN();
        hash.update(canonical_nan);
    }
    else if (x == static_cast<Float64>(0))
    {
        static constexpr Float64 positive_zero = static_cast<Float64>(0);
        hash.update(positive_zero);
    }
    else
    {
        hash.update(x);
    }
}

void FieldVisitorHash::operator() (const String & x) const
{
    UInt8 type = Field::Types::String;
    hash.update(type);
    hash.update(x.size());
    hash.update(x.data(), x.size());
}

void FieldVisitorHash::operator() (const Tuple & x) const
{
    UInt8 type = Field::Types::Tuple;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const Map & x) const
{
    UInt8 type = Field::Types::Map;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const Array & x) const
{
    UInt8 type = Field::Types::Array;
    hash.update(type);
    hash.update(x.size());

    for (const auto & elem : x)
        applyVisitor(*this, elem);
}

void FieldVisitorHash::operator() (const Object & x) const
{
    UInt8 type = Field::Types::Object;
    hash.update(type);
    hash.update(x.size());

    for (const auto & [key, value]: x)
    {
        hash.update(key);
        applyVisitor(*this, value);
    }
}

template <typename T>
static void hashDecimalNormalized(SipHash & hash, UInt8 type, const DecimalField<T> & x)
{
    hash.update(type);
    auto v = x.getValue().value;
    UInt32 s = x.getScale();
    DecimalUtils::normalizeDecimalTrailingZeros(v, s);
    hash.update(v);
    hash.update(s);
}

void FieldVisitorHash::operator() (const DecimalField<Decimal32> & x) const
{
    hashDecimalNormalized(hash, Field::Types::Decimal32, x);
}

void FieldVisitorHash::operator() (const DecimalField<Decimal64> & x) const
{
    hashDecimalNormalized(hash, Field::Types::Decimal64, x);
}

void FieldVisitorHash::operator() (const DecimalField<Decimal128> & x) const
{
    hashDecimalNormalized(hash, Field::Types::Decimal128, x);
}

void FieldVisitorHash::operator() (const DecimalField<Decimal256> & x) const
{
    hashDecimalNormalized(hash, Field::Types::Decimal256, x);
}

void FieldVisitorHash::operator() (const AggregateFunctionStateData & x) const
{
    UInt8 type = Field::Types::AggregateFunctionState;
    hash.update(type);
    hash.update(x.name.size());
    hash.update(x.name.data(), x.name.size());
    hash.update(x.data.size());
    hash.update(x.data.data(), x.data.size());
}

void FieldVisitorHash::operator() (const UInt256 & x) const
{
    UInt8 type = Field::Types::UInt256;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const Int256 & x) const
{
    UInt8 type = Field::Types::Int256;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const bool & x) const
{
    UInt8 type = Field::Types::Bool;
    hash.update(type);
    hash.update(x);
}

void FieldVisitorHash::operator() (const CustomType & x) const
{
    UInt8 type = Field::Types::CustomType;
    hash.update(type);
    hash.update(x.getTypeName());
    auto result = x.toString();
    hash.update(result.size());
    hash.update(result.data(), result.size());
}

}
