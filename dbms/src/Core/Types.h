#pragma once

#include <cstdint>
#include <string>
#include <vector>


namespace DB
{

/// Data types for representing elementary values from a database in RAM.

struct Null {};

enum class TypeIndex
{
    Nothing = 0,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Float32,
    Float64,
    Date,
    DateTime,
    String,
    FixedString,
    Enum8,
    Enum16,
    Decimal32,
    Decimal64,
    Decimal128,
    UUID,
    Array,
    Tuple,
    Set,
    Interval,
    Nullable,
    Function,
    AggregateFunction,
    LowCardinality,
};

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

using Float32 = float;
using Float64 = double;

using String = std::string;


/** Note that for types not used in DB, IsNumber is false.
  */
template <typename T> constexpr bool IsNumber = false;

template <> inline constexpr bool IsNumber<UInt8> = true;
template <> inline constexpr bool IsNumber<UInt16> = true;
template <> inline constexpr bool IsNumber<UInt32> = true;
template <> inline constexpr bool IsNumber<UInt64> = true;
template <> inline constexpr bool IsNumber<Int8> = true;
template <> inline constexpr bool IsNumber<Int16> = true;
template <> inline constexpr bool IsNumber<Int32> = true;
template <> inline constexpr bool IsNumber<Int64> = true;
template <> inline constexpr bool IsNumber<Float32> = true;
template <> inline constexpr bool IsNumber<Float64> = true;

template <typename T> struct TypeName;

template <> struct TypeName<UInt8>   { static const char * get() { return "UInt8";   } };
template <> struct TypeName<UInt16>  { static const char * get() { return "UInt16";  } };
template <> struct TypeName<UInt32>  { static const char * get() { return "UInt32";  } };
template <> struct TypeName<UInt64>  { static const char * get() { return "UInt64";  } };
template <> struct TypeName<Int8>    { static const char * get() { return "Int8";    } };
template <> struct TypeName<Int16>   { static const char * get() { return "Int16";   } };
template <> struct TypeName<Int32>   { static const char * get() { return "Int32";   } };
template <> struct TypeName<Int64>   { static const char * get() { return "Int64";   } };
template <> struct TypeName<Float32> { static const char * get() { return "Float32"; } };
template <> struct TypeName<Float64> { static const char * get() { return "Float64"; } };
template <> struct TypeName<String>  { static const char * get() { return "String";  } };

template <typename T> struct TypeId;
template <> struct TypeId<UInt8>    { static constexpr const TypeIndex value = TypeIndex::UInt8;  };
template <> struct TypeId<UInt16>   { static constexpr const TypeIndex value = TypeIndex::UInt16;  };
template <> struct TypeId<UInt32>   { static constexpr const TypeIndex value = TypeIndex::UInt32;  };
template <> struct TypeId<UInt64>   { static constexpr const TypeIndex value = TypeIndex::UInt64;  };
template <> struct TypeId<Int8>     { static constexpr const TypeIndex value = TypeIndex::Int8;  };
template <> struct TypeId<Int16>    { static constexpr const TypeIndex value = TypeIndex::Int16; };
template <> struct TypeId<Int32>    { static constexpr const TypeIndex value = TypeIndex::Int32; };
template <> struct TypeId<Int64>    { static constexpr const TypeIndex value = TypeIndex::Int64; };
template <> struct TypeId<Float32>  { static constexpr const TypeIndex value = TypeIndex::Float32;  };
template <> struct TypeId<Float64>  { static constexpr const TypeIndex value = TypeIndex::Float64;  };

/// Not a data type in database, defined just for convenience.
using Strings = std::vector<String>;


using Int128 = __int128;
template <> inline constexpr bool IsNumber<Int128> = true;
template <> struct TypeName<Int128> { static const char * get() { return "Int128";  } };
template <> struct TypeId<Int128>   { static constexpr const TypeIndex value = TypeIndex::Int128; };

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
    :   value(x)
    {}

    constexpr Decimal<T> & operator = (Decimal<T> &&) = default;
    constexpr Decimal<T> & operator = (const Decimal<T> &) = default;

    operator T () const { return value; }

    const Decimal<T> & operator += (const T & x) { value += x; return *this; }
    const Decimal<T> & operator -= (const T & x) { value -= x; return *this; }
    const Decimal<T> & operator *= (const T & x) { value *= x; return *this; }
    const Decimal<T> & operator /= (const T & x) { value /= x; return *this; }
    const Decimal<T> & operator %= (const T & x) { value %= x; return *this; }

    T value;
};

using Decimal32 = Decimal<Int32>;
using Decimal64 = Decimal<Int64>;
using Decimal128 = Decimal<Int128>;

template <> struct TypeName<Decimal32>   { static const char * get() { return "Decimal32";   } };
template <> struct TypeName<Decimal64>   { static const char * get() { return "Decimal64";   } };
template <> struct TypeName<Decimal128>  { static const char * get() { return "Decimal128";  } };

template <> struct TypeId<Decimal32>    { static constexpr const TypeIndex value = TypeIndex::Decimal32; };
template <> struct TypeId<Decimal64>    { static constexpr const TypeIndex value = TypeIndex::Decimal64; };
template <> struct TypeId<Decimal128>   { static constexpr const TypeIndex value = TypeIndex::Decimal128; };

template <typename T> constexpr bool IsDecimalNumber = false;
template <> inline constexpr bool IsDecimalNumber<Decimal32> = true;
template <> inline constexpr bool IsDecimalNumber<Decimal64> = true;
template <> inline constexpr bool IsDecimalNumber<Decimal128> = true;

template <typename T> struct NativeType { using Type = T; };
template <> struct NativeType<Decimal32> { using Type = Int32; };
template <> struct NativeType<Decimal64> { using Type = Int64; };
template <> struct NativeType<Decimal128> { using Type = Int128; };

inline const char * getTypeName(TypeIndex idx)
{
    switch (idx)
    {
        case TypeIndex::Nothing:    return "Nothing";
        case TypeIndex::UInt8:      return TypeName<UInt8>::get();
        case TypeIndex::UInt16:     return TypeName<UInt16>::get();
        case TypeIndex::UInt32:     return TypeName<UInt32>::get();
        case TypeIndex::UInt64:     return TypeName<UInt64>::get();
        case TypeIndex::UInt128:    return "UInt128";
        case TypeIndex::Int8:       return TypeName<Int8>::get();
        case TypeIndex::Int16:      return TypeName<Int16>::get();
        case TypeIndex::Int32:      return TypeName<Int32>::get();
        case TypeIndex::Int64:      return TypeName<Int64>::get();
        case TypeIndex::Int128:     return TypeName<Int128>::get();
        case TypeIndex::Float32:    return TypeName<Float32>::get();
        case TypeIndex::Float64:    return TypeName<Float64>::get();
        case TypeIndex::Date:       return "Date";
        case TypeIndex::DateTime:   return "DateTime";
        case TypeIndex::String:     return TypeName<String>::get();
        case TypeIndex::FixedString: return "FixedString";
        case TypeIndex::Enum8:      return "Enum8";
        case TypeIndex::Enum16:     return "Enum16";
        case TypeIndex::Decimal32:  return TypeName<Decimal32>::get();
        case TypeIndex::Decimal64:  return TypeName<Decimal64>::get();
        case TypeIndex::Decimal128: return TypeName<Decimal128>::get();
        case TypeIndex::UUID:       return "UUID";
        case TypeIndex::Array:      return "Array";
        case TypeIndex::Tuple:      return "Tuple";
        case TypeIndex::Set:        return "Set";
        case TypeIndex::Interval:   return "Interval";
        case TypeIndex::Nullable:   return "Nullable";
        case TypeIndex::Function:   return "Function";
        case TypeIndex::AggregateFunction: return "AggregateFunction";
        case TypeIndex::LowCardinality: return "LowCardinality";
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
}
