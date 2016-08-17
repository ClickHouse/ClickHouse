#include <DB/DataTypes/NullSymbol.h>

namespace DB
{

namespace NullSymbol
{

constexpr decltype(Plain::name) Plain::name;
constexpr decltype(Escaped::name) Escaped::name;
constexpr decltype(Quoted::name) Quoted::name;
constexpr decltype(CSV::name) CSV::name;
constexpr decltype(JSON::name) JSON::name;
constexpr decltype(XML::name) XML::name;

}

}
