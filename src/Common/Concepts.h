#pragma once

#include <concepts>
#include <variant>
#include <type_traits>

namespace DB
{

template<typename T, typename ... U>
concept is_any_of = (std::same_as<T, U> || ...);


template <typename... T>
concept OptionalArgument = requires(T &&...)
{
    requires(sizeof...(T) == 0 || sizeof...(T) == 1);
};

template <typename T, typename Variant>
struct IsInVariant : std::false_type {};

template <typename T, typename... Ts>
struct IsInVariant<T, std::variant<Ts...>> : std::disjunction<std::is_same<T, Ts>...>
{
};

template <typename T, typename Variant>
concept InVariant = IsInVariant<T, Variant>::value;

}
