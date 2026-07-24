#pragma once

#include <map>
#include <list>
#include <optional>
#include <string>
#include <set>
#include <initializer_list>

#include <DataTypes/IDataType.h>
#include <Core/Names.h>

namespace DB
{

class NameAndAliasPair
{
public:
    NameAndAliasPair(const String & name_, const DataTypePtr & type_, const String & expression_)
        : name(name_)
        , type(type_)
        , expression(expression_)
    {}

    String name;
    DataTypePtr type;
    String expression;
};

/// This needed to use structured bindings for NameAndTypePair
/// const auto & [name, type] = name_and_type
template <int I>
decltype(auto) get(const NameAndAliasPair & name_and_alias)
{
    if constexpr (I == 0)
        return name_and_alias.name;
    else if constexpr (I == 1)
        return name_and_alias.type;
    else if constexpr (I == 2)
        return name_and_alias.expression;
}

using NamesAndAliases = std::vector<NameAndAliasPair>;

}

namespace std
{
    template <> struct tuple_size<DB::NameAndAliasPair> : std::integral_constant<size_t, 2> {};
    template <> struct tuple_element<0, DB::NameAndAliasPair> { using type = String; };
    template <> struct tuple_element<1, DB::NameAndAliasPair> { using type = DB::DataTypePtr; };
    template <> struct tuple_element<2, DB::NameAndAliasPair> { using type = String; };
}
