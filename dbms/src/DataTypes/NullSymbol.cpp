#include <DB/DataTypes/NullSymbol.h>

namespace DB
{

namespace NullSymbol
{

constexpr decltype(Plain::prefix) Plain::prefix;
constexpr decltype(Plain::suffix) Plain::suffix;
constexpr decltype(Plain::name) Plain::name;
constexpr decltype(Plain::length) Plain::length;

constexpr decltype(Escaped::prefix) Escaped::prefix;
constexpr decltype(Escaped::suffix) Escaped::suffix;
constexpr decltype(Escaped::name) Escaped::name;
constexpr decltype(Escaped::length) Escaped::length;

constexpr decltype(Quoted::prefix) Quoted::prefix;
constexpr decltype(Quoted::suffix) Quoted::suffix;
constexpr decltype(Quoted::name) Quoted::name;
constexpr decltype(Quoted::length) Quoted::length;

constexpr decltype(CSV::prefix) CSV::prefix;
constexpr decltype(CSV::suffix) CSV::suffix;
constexpr decltype(CSV::name) CSV::name;
constexpr decltype(CSV::length) CSV::length;

constexpr decltype(JSON::prefix) JSON::prefix;
constexpr decltype(JSON::suffix) JSON::suffix;
constexpr decltype(JSON::name) JSON::name;
constexpr decltype(JSON::length) JSON::length;

constexpr decltype(XML::prefix) XML::prefix;
constexpr decltype(XML::suffix) XML::suffix;
constexpr decltype(XML::name) XML::name;
constexpr decltype(XML::length) XML::length;

}

}
