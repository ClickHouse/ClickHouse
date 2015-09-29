#pragma once

#include <string>
#include <vector>
#include <Poco/Types.h>
#include <common/strong_typedef.h>


namespace DB
{

/** Типы данных для представления значений из БД в оперативке.
  */

STRONG_TYPEDEF(char, Null);

typedef Poco::UInt8 UInt8;
typedef Poco::UInt16 UInt16;
typedef Poco::UInt32 UInt32;
typedef Poco::UInt64 UInt64;

typedef Poco::Int8 Int8;
typedef Poco::Int16 Int16;
typedef Poco::Int32 Int32;
typedef Poco::Int64 Int64;

typedef float Float32;
typedef double Float64;

typedef std::string String;
typedef std::vector<String> Strings;


template <typename T> struct IsNumber 	{ static const bool value = false; };

template <> struct IsNumber<UInt8> 		{ static const bool value = true; };
template <> struct IsNumber<UInt16> 	{ static const bool value = true; };
template <> struct IsNumber<UInt32> 	{ static const bool value = true; };
template <> struct IsNumber<UInt64> 	{ static const bool value = true; };
template <> struct IsNumber<Int8> 		{ static const bool value = true; };
template <> struct IsNumber<Int16> 		{ static const bool value = true; };
template <> struct IsNumber<Int32> 		{ static const bool value = true; };
template <> struct IsNumber<Int64> 		{ static const bool value = true; };
template <> struct IsNumber<Float32> 	{ static const bool value = true; };
template <> struct IsNumber<Float64> 	{ static const bool value = true; };


template <typename T> struct TypeName;

template <> struct TypeName<Null> 		{ static std::string get() { return "Null";		} };
template <> struct TypeName<UInt8> 		{ static std::string get() { return "UInt8";	} };
template <> struct TypeName<UInt16> 	{ static std::string get() { return "UInt16"; 	} };
template <> struct TypeName<UInt32> 	{ static std::string get() { return "UInt32"; 	} };
template <> struct TypeName<UInt64> 	{ static std::string get() { return "UInt64"; 	} };
template <> struct TypeName<Int8> 		{ static std::string get() { return "Int8"; 	} };
template <> struct TypeName<Int16> 		{ static std::string get() { return "Int16";	} };
template <> struct TypeName<Int32> 		{ static std::string get() { return "Int32";	} };
template <> struct TypeName<Int64> 		{ static std::string get() { return "Int64";	} };
template <> struct TypeName<Float32> 	{ static std::string get() { return "Float32"; 	} };
template <> struct TypeName<Float64> 	{ static std::string get() { return "Float64"; 	} };
template <> struct TypeName<String> 	{ static std::string get() { return "String"; 	} };

}
