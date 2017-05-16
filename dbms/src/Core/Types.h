#pragma once

#include <string>
#include <vector>
#include <Poco/Types.h>
#include <common/strong_typedef.h>


namespace DB
{

/** Data types for representing values from a database in RAM.
  */

STRONG_TYPEDEF(char, Null);

using UInt8 = Poco::UInt8;
using UInt16 = Poco::UInt16;
using UInt32 = Poco::UInt32;
using UInt64 = Poco::UInt64;

using Int8 = Poco::Int8;
using Int16 = Poco::Int16;
using Int32 = Poco::Int32;
using Int64 = Poco::Int64;

using Float32 = float;
using Float64 = double;

using String = std::string;
using Strings = std::vector<String>;

/// Ordinary types with nullability.
template <typename T> struct Nullable { using Type = T; };

/// Get a non-nullable type.
template <typename T> struct RemoveNullable { using Type = T; };
template <typename T> struct RemoveNullable<Nullable<T>> { using Type = T; };

/// Check if a type is nullable.
template <typename T> struct IsNullable { static constexpr bool value = false; };
template <typename T> struct IsNullable<Nullable<T>> { static constexpr bool value = true; };

template <typename T> struct IsNumber     { static constexpr bool value = false; };
template <typename T> struct IsNumber<Nullable<T> > { static constexpr bool value = IsNumber<T>::value; };

template <> struct IsNumber<UInt8>         { static constexpr bool value = true; };
template <> struct IsNumber<UInt16>     { static constexpr bool value = true; };
template <> struct IsNumber<UInt32>     { static constexpr bool value = true; };
template <> struct IsNumber<UInt64>     { static constexpr bool value = true; };
template <> struct IsNumber<Int8>         { static constexpr bool value = true; };
template <> struct IsNumber<Int16>         { static constexpr bool value = true; };
template <> struct IsNumber<Int32>         { static constexpr bool value = true; };
template <> struct IsNumber<Int64>         { static constexpr bool value = true; };
template <> struct IsNumber<Float32>     { static constexpr bool value = true; };
template <> struct IsNumber<Float64>     { static constexpr bool value = true; };


template <typename T> struct TypeName;
template <typename T> struct TypeName<Nullable<T>> { static std::string get() { return "Nullable(" + TypeName<T>::get() + ")"; } };

template <> struct TypeName<Null>         { static std::string get() { return "Null"; } };
template <> struct TypeName<Nullable<void>> : TypeName<Null> {};

template <> struct TypeName<UInt8>         { static std::string get() { return "UInt8";    } };
template <> struct TypeName<UInt16>     { static std::string get() { return "UInt16";     } };
template <> struct TypeName<UInt32>     { static std::string get() { return "UInt32";     } };
template <> struct TypeName<UInt64>     { static std::string get() { return "UInt64";     } };
template <> struct TypeName<Int8>         { static std::string get() { return "Int8";     } };
template <> struct TypeName<Int16>         { static std::string get() { return "Int16";    } };
template <> struct TypeName<Int32>         { static std::string get() { return "Int32";    } };
template <> struct TypeName<Int64>         { static std::string get() { return "Int64";    } };
template <> struct TypeName<Float32>     { static std::string get() { return "Float32";     } };
template <> struct TypeName<Float64>     { static std::string get() { return "Float64";     } };
template <> struct TypeName<String>     { static std::string get() { return "String";     } };

/// This type is not supported by the DBMS, but is used in some internal transformations.
template <> struct TypeName<long double>{ static std::string get() { return "long double";     } };

}
