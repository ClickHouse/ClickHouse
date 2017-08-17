#pragma once

namespace DB
{

template <typename ... Types>
struct TypeList;

template <typename Type, typename ... Types>
struct TypeList<Type, Types...>
{
    using Head = Type;
    using Tail = TypeList<Types ...>;
};

template<>
struct TypeList<>
{
};

/// Append Type to TypeList
/// Usage:
///     using TypeListWithType = typename AppendToTypeList<Type, ConcreteTypeList>::Type;
template <typename TypeToAppend, typename List, typename ... Types>
struct AppendToTypeList
{
    using Type = typename AppendToTypeList<TypeToAppend, typename List::Tail, Types ..., typename List::Head>::Type;
};

template <typename TypeToAppend, typename ... Types>
struct AppendToTypeList<TypeToAppend, TypeList<>, Types ...>
{
    using Type = TypeList<TypeToAppend, Types ...>;
};

/// Apply TypeList as variadic template argument of Class.
/// Usage:
///     using ClassWithAppliedTypeList = typename ApplyTypeListForClass<Class, ConcreteTypeList>::Type;
template <template <typename ...> typename Class, typename List, typename ... Types>
struct ApplyTypeListForClass
{
    using Type = typename ApplyTypeListForClass<Class, typename List::Tail, Types ..., typename List::Head>::Type;
};

template <template <typename ...> typename Class, typename ... Types>
struct ApplyTypeListForClass<Class, TypeList<>, Types ...>
{
    using Type = Class<Types ...>;
};

}
