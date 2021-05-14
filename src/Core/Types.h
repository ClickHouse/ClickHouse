#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <common/strong_typedef.h>
#include <common/extended_types.h>
#include <common/defines.h>


namespace DB
{

/// Data types for representing elementary values from a database in RAM.

struct Null {};

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
};
#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif


using UInt128 = ::UInt128;
using UInt256 = ::UInt256;
using Int128 = ::Int128;
using Int256 = ::Int256;

STRONG_TYPEDEF(UInt128, UUID)


template <typename T> constexpr const char * TypeName = "";

template <> inline constexpr const char * TypeName<UInt8> = "UInt8";
template <> inline constexpr const char * TypeName<UInt16> = "UInt16";
template <> inline constexpr const char * TypeName<UInt32> = "UInt32";
template <> inline constexpr const char * TypeName<UInt64> = "UInt64";
template <> inline constexpr const char * TypeName<UInt128> = "UInt128";
template <> inline constexpr const char * TypeName<UInt256> = "UInt256";
template <> inline constexpr const char * TypeName<Int8> = "Int8";
template <> inline constexpr const char * TypeName<Int16> = "Int16";
template <> inline constexpr const char * TypeName<Int32> = "Int32";
template <> inline constexpr const char * TypeName<Int64> = "Int64";
template <> inline constexpr const char * TypeName<Int128> = "Int128";
template <> inline constexpr const char * TypeName<Int256> = "Int256";
template <> inline constexpr const char * TypeName<Float32> = "Float32";
template <> inline constexpr const char * TypeName<Float64> = "Float64";
template <> inline constexpr const char * TypeName<String> = "String";
template <> inline constexpr const char * TypeName<UUID> = "UUID";

/// TODO Try to remove it.
template <typename T> constexpr TypeIndex TypeId = TypeIndex::Nothing;
template <> inline constexpr TypeIndex TypeId<UInt8> = TypeIndex::UInt8;
template <> inline constexpr TypeIndex TypeId<UInt16> = TypeIndex::UInt16;
template <> inline constexpr TypeIndex TypeId<UInt32> = TypeIndex::UInt32;
template <> inline constexpr TypeIndex TypeId<UInt64> = TypeIndex::UInt64;
template <> inline constexpr TypeIndex TypeId<UInt128> = TypeIndex::UInt128;
template <> inline constexpr TypeIndex TypeId<UInt256> = TypeIndex::UInt256;
template <> inline constexpr TypeIndex TypeId<Int8> = TypeIndex::Int8;
template <> inline constexpr TypeIndex TypeId<Int16> = TypeIndex::Int16;
template <> inline constexpr TypeIndex TypeId<Int32> = TypeIndex::Int32;
template <> inline constexpr TypeIndex TypeId<Int64> = TypeIndex::Int64;
template <> inline constexpr TypeIndex TypeId<Int128> = TypeIndex::Int128;
template <> inline constexpr TypeIndex TypeId<Int256> = TypeIndex::Int256;
template <> inline constexpr TypeIndex TypeId<Float32> = TypeIndex::Float32;
template <> inline constexpr TypeIndex TypeId<Float64> = TypeIndex::Float64;
template <> inline constexpr TypeIndex TypeId<UUID> = TypeIndex::UUID;


/// Not a data type in database, defined just for convenience.
using Strings = std::vector<String>;

/// Own FieldType for Decimal.
/// It is only a "storage" for decimal. To perform operations, you also have to provide a scale (number of digits after point).
template <typename T>
struct Decimal
{
    using NativeType = T;

    Decimal() = default;
    Decimal(Decimal<T> &&) = default;
    Decimal(const Decimal<T> &) = default;

    Decimal(const T & value_)
    :   value(value_)
    {}

    template <typename U>
    Decimal(const Decimal<U> & x)
    :   value(x.value)
    {}

    constexpr Decimal<T> & operator = (Decimal<T> &&) = default;
    constexpr Decimal<T> & operator = (const Decimal<T> &) = default;

    operator T () const { return value; }

    template <typename U>
    U convertTo() const
    {
        /// no IsDecimalNumber defined yet
        if constexpr (std::is_same_v<U, Decimal<Int32>> ||
                      std::is_same_v<U, Decimal<Int64>> ||
                      std::is_same_v<U, Decimal<Int128>> ||
                      std::is_same_v<U, Decimal<Int256>>)
        {
            return convertTo<typename U::NativeType>();
        }
        else
            return static_cast<U>(value);
    }

    const Decimal<T> & operator += (const T & x) { value += x; return *this; }
    const Decimal<T> & operator -= (const T & x) { value -= x; return *this; }
    const Decimal<T> & operator *= (const T & x) { value *= x; return *this; }
    const Decimal<T> & operator /= (const T & x) { value /= x; return *this; }
    const Decimal<T> & operator %= (const T & x) { value %= x; return *this; }

    template <typename U> const Decimal<T> & operator += (const Decimal<U> & x) { value += x.value; return *this; }
    template <typename U> const Decimal<T> & operator -= (const Decimal<U> & x) { value -= x.value; return *this; }
    template <typename U> const Decimal<T> & operator *= (const Decimal<U> & x) { value *= x.value; return *this; }
    template <typename U> const Decimal<T> & operator /= (const Decimal<U> & x) { value /= x.value; return *this; }
    template <typename U> const Decimal<T> & operator %= (const Decimal<U> & x) { value %= x.value; return *this; }

    /// This is to avoid UB for sumWithOverflow()
    void NO_SANITIZE_UNDEFINED addOverflow(const T & x) { value += x; }

    T value;
};

template <typename T> inline bool operator< (const Decimal<T> & x, const Decimal<T> & y) { return x.value < y.value; }
template <typename T> inline bool operator> (const Decimal<T> & x, const Decimal<T> & y) { return x.value > y.value; }
template <typename T> inline bool operator<= (const Decimal<T> & x, const Decimal<T> & y) { return x.value <= y.value; }
template <typename T> inline bool operator>= (const Decimal<T> & x, const Decimal<T> & y) { return x.value >= y.value; }
template <typename T> inline bool operator== (const Decimal<T> & x, const Decimal<T> & y) { return x.value == y.value; }
template <typename T> inline bool operator!= (const Decimal<T> & x, const Decimal<T> & y) { return x.value != y.value; }

template <typename T> inline Decimal<T> operator+ (const Decimal<T> & x, const Decimal<T> & y) { return x.value + y.value; }
template <typename T> inline Decimal<T> operator- (const Decimal<T> & x, const Decimal<T> & y) { return x.value - y.value; }
template <typename T> inline Decimal<T> operator* (const Decimal<T> & x, const Decimal<T> & y) { return x.value * y.value; }
template <typename T> inline Decimal<T> operator/ (const Decimal<T> & x, const Decimal<T> & y) { return x.value / y.value; }
template <typename T> inline Decimal<T> operator- (const Decimal<T> & x) { return -x.value; }

using Decimal32 = Decimal<Int32>;
using Decimal64 = Decimal<Int64>;
using Decimal128 = Decimal<Int128>;
using Decimal256 = Decimal<Int256>;

// Distinguishable type to allow function resolution/deduction based on value type,
// but also relatively easy to convert to/from Decimal64.
class DateTime64 : public Decimal64
{
public:
    using Base = Decimal64;
    using Base::Base;

    DateTime64(const Base & v)
        : Base(v)
    {}
};

template <> inline constexpr const char * TypeName<Decimal32> = "Decimal32";
template <> inline constexpr const char * TypeName<Decimal64> = "Decimal64";
template <> inline constexpr const char * TypeName<Decimal128> = "Decimal128";
template <> inline constexpr const char * TypeName<Decimal256> = "Decimal256";
template <> inline constexpr const char * TypeName<DateTime64> = "DateTime64";

template <> inline constexpr TypeIndex TypeId<Decimal32> = TypeIndex::Decimal32;
template <> inline constexpr TypeIndex TypeId<Decimal64> = TypeIndex::Decimal64;
template <> inline constexpr TypeIndex TypeId<Decimal128> = TypeIndex::Decimal128;
template <> inline constexpr TypeIndex TypeId<Decimal256> = TypeIndex::Decimal256;
template <> inline constexpr TypeIndex TypeId<DateTime64> = TypeIndex::DateTime64;

template <typename T> constexpr bool IsDecimalNumber = false;
template <> inline constexpr bool IsDecimalNumber<Decimal32> = true;
template <> inline constexpr bool IsDecimalNumber<Decimal64> = true;
template <> inline constexpr bool IsDecimalNumber<Decimal128> = true;
template <> inline constexpr bool IsDecimalNumber<Decimal256> = true;
template <> inline constexpr bool IsDecimalNumber<DateTime64> = true;

template <typename T> struct NativeType { using Type = T; };
template <> struct NativeType<Decimal32> { using Type = Int32; };
template <> struct NativeType<Decimal64> { using Type = Int64; };
template <> struct NativeType<Decimal128> { using Type = Int128; };
template <> struct NativeType<Decimal256> { using Type = Int256; };
template <> struct NativeType<DateTime64> { using Type = Int64; };

template <typename T> constexpr bool OverBigInt = false;
template <> inline constexpr bool OverBigInt<Int128> = true;
template <> inline constexpr bool OverBigInt<UInt128> = true;
template <> inline constexpr bool OverBigInt<Int256> = true;
template <> inline constexpr bool OverBigInt<UInt256> = true;
template <> inline constexpr bool OverBigInt<Decimal128> = true;
template <> inline constexpr bool OverBigInt<Decimal256> = true;

inline constexpr const char * getTypeName(TypeIndex idx)
{
    switch (idx)
    {
        case TypeIndex::Nothing:    return "Nothing";
        case TypeIndex::UInt8:      return "UInt8";
        case TypeIndex::UInt16:     return "UInt16";
        case TypeIndex::UInt32:     return "UInt32";
        case TypeIndex::UInt64:     return "UInt64";
        case TypeIndex::UInt128:    return "UInt128";
        case TypeIndex::UInt256:    return "UInt256";
        case TypeIndex::Int8:       return "Int8";
        case TypeIndex::Int16:      return "Int16";
        case TypeIndex::Int32:      return "Int32";
        case TypeIndex::Int64:      return "Int64";
        case TypeIndex::Int128:     return "Int128";
        case TypeIndex::Int256:     return "Int256";
        case TypeIndex::Float32:    return "Float32";
        case TypeIndex::Float64:    return "Float64";
        case TypeIndex::Date:       return "Date";
        case TypeIndex::DateTime:   return "DateTime";
        case TypeIndex::DateTime64: return "DateTime64";
        case TypeIndex::String:     return "String";
        case TypeIndex::FixedString: return "FixedString";
        case TypeIndex::Enum8:      return "Enum8";
        case TypeIndex::Enum16:     return "Enum16";
        case TypeIndex::Decimal32:  return "Decimal32";
        case TypeIndex::Decimal64:  return "Decimal64";
        case TypeIndex::Decimal128: return "Decimal128";
        case TypeIndex::Decimal256: return "Decimal256";
        case TypeIndex::UUID:       return "UUID";
        case TypeIndex::Array:      return "Array";
        case TypeIndex::Tuple:      return "Tuple";
        case TypeIndex::Set:        return "Set";
        case TypeIndex::Interval:   return "Interval";
        case TypeIndex::Nullable:   return "Nullable";
        case TypeIndex::Function:   return "Function";
        case TypeIndex::AggregateFunction: return "AggregateFunction";
        case TypeIndex::LowCardinality: return "LowCardinality";
        case TypeIndex::Map:        return "Map";
    }

    __builtin_unreachable();
}

}

/// Specialization of `std::hash` for the Decimal<T> types.
namespace std
{
    template <typename T>
    struct hash<DB::Decimal<T>> { size_t operator()(const DB::Decimal<T> & x) const { return hash<T>()(x.value); } };

    template <>
    struct hash<DB::Decimal128>
    {
        size_t operator()(const DB::Decimal128 & x) const
        {
            return std::hash<DB::Int64>()(x.value >> 64)
                ^ std::hash<DB::Int64>()(x.value & std::numeric_limits<DB::UInt64>::max());
        }
    };

    template <>
    struct hash<DB::DateTime64>
    {
        size_t operator()(const DB::DateTime64 & x) const
        {
            return std::hash<std::decay_t<decltype(x)>::NativeType>()(x);
        }
    };


    template <>
    struct hash<DB::Decimal256>
    {
        size_t operator()(const DB::Decimal256 & x) const
        {
            // temp solution
            static constexpr DB::UInt64 max_uint_mask = std::numeric_limits<DB::UInt64>::max();
            return std::hash<DB::Int64>()(static_cast<DB::Int64>(x.value >> 64 & max_uint_mask))
                ^ std::hash<DB::Int64>()(static_cast<DB::Int64>(x.value & max_uint_mask));
        }
    };
}
