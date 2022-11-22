#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <base/strong_typedef.h>
#include <base/Decimal.h>
#include <base/defines.h>
#include <base/UUID.h>


namespace DB
{

/// Data types for representing elementary values from a database in RAM.

/// Hold a null value for untyped calculation. It can also store infinities to handle nullable
/// comparison which is used for nullable KeyCondition.
struct Null
{
    enum class Value
    {
        Null,
        PositiveInfinity,
        NegativeInfinity,
    };

    Value value{Value::Null};

    bool isNull() const { return value == Value::Null; }
    bool isPositiveInfinity() const { return value == Value::PositiveInfinity; }
    bool isNegativeInfinity() const { return value == Value::NegativeInfinity; }

    bool operator==(const Null & other) const
    {
        return value == other.value;
    }

    bool operator!=(const Null & other) const
    {
        return !(*this == other);
    }
};

/// Ignore strange gcc warning https://gcc.gnu.org/bugzilla/show_bug.cgi?id=55776
#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow"
#endif
/// @note Except explicitly described you should not assume on TypeIndex numbers and/or their orders in this enum.
enum class TypeIndex
{
    Nothing = 0,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    Date,
    Date32,
    DateTime,
    DateTime64,
    String,
    FixedString,
    Enum8,
    Enum16,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    UUID,
    Array,
    Tuple,
    Set,
    Interval,
    Nullable,
    Function,
    AggregateFunction,
    LowCardinality,
    Map,
    Object,
};
#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif


using UInt128 = ::UInt128;
using UInt256 = ::UInt256;
using Int128 = ::Int128;
using Int256 = ::Int256;

/// Not a data type in database, defined just for convenience.
using Strings = std::vector<String>;
}
