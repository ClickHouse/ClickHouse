#pragma once

#include <string>
#include <vector>
#include <cstdint>


namespace DB
{

/// Data types for representing elementary values from a database in RAM.

struct Null {};

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

template <> constexpr bool IsNumber<UInt8> = true;
template <> constexpr bool IsNumber<UInt16> = true;
template <> constexpr bool IsNumber<UInt32> = true;
template <> constexpr bool IsNumber<UInt64> = true;
template <> constexpr bool IsNumber<Int8> = true;
template <> constexpr bool IsNumber<Int16> = true;
template <> constexpr bool IsNumber<Int32> = true;
template <> constexpr bool IsNumber<Int64> = true;
template <> constexpr bool IsNumber<Float32> = true;
template <> constexpr bool IsNumber<Float64> = true;

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

/// 0 reserved for types without number
template <> struct TypeId<UInt8>    { static constexpr const size_t value = 1;  };
template <> struct TypeId<UInt16>   { static constexpr const size_t value = 2;  };
template <> struct TypeId<UInt32>   { static constexpr const size_t value = 3;  };
template <> struct TypeId<UInt64>   { static constexpr const size_t value = 4;  };
/// 5 reserved for TypeId<UInt128>
template <> struct TypeId<Float32>  { static constexpr const size_t value = 7;  };
template <> struct TypeId<Float64>  { static constexpr const size_t value = 8;  };
template <> struct TypeId<Int8>     { static constexpr const size_t value = 9;  };
template <> struct TypeId<Int16>    { static constexpr const size_t value = 10; };
template <> struct TypeId<Int32>    { static constexpr const size_t value = 11; };
template <> struct TypeId<Int64>    { static constexpr const size_t value = 12; };
/// 13 reserved for TypeId<Int128>

/// Not a data type in database, defined just for convenience.
using Strings = std::vector<String>;

}

#if 1 /// __int128
namespace DB
{

using Int128 = __int128;
template <> constexpr bool IsNumber<Int128> = true;
template <> struct TypeName<Int128>  { static const char * get() { return "Int128";  } };
template <> struct TypeId<Int128>   { static constexpr const size_t value = 13; };

}

namespace std
{

template <> struct is_signed<__int128>
{
    static constexpr bool value = true;
};

template <> struct is_unsigned<__int128>
{
    static constexpr bool value = false;
};

template <> struct is_integral<__int128>
{
    static constexpr bool value = true;
};

template <> struct is_arithmetic<__int128>
{
    static constexpr bool value = true;
};

}
#endif


namespace DB
{
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

    template <> struct TypeId<Decimal32>    { static constexpr const size_t value = 16; };
    template <> struct TypeId<Decimal64>    { static constexpr const size_t value = 17; };
    template <> struct TypeId<Decimal128>   { static constexpr const size_t value = 18; };

    template <typename T>
    constexpr bool IsDecimalNumber = false;
    template <> constexpr bool IsDecimalNumber<Decimal32> = true;
    template <> constexpr bool IsDecimalNumber<Decimal64> = true;
    template <> constexpr bool IsDecimalNumber<Decimal128> = true;
}
