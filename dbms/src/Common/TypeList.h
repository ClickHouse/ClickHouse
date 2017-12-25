#pragma once

#include <cstddef>
#include <utility>
#include <Core/Defines.h>

namespace DB
{

template <typename... TTail>
struct TypeList
{
    static constexpr size_t size = 0;

    template <size_t I>
    using At = std::nullptr_t;

    template <typename Func, size_t index = 0>
    static void forEach(Func && /*func*/)
    {
    }
};


template <typename THead, typename... TTail>
struct TypeList<THead, TTail...>
{
    using Head = THead;
    using Tail = TypeList<TTail...>;

    static constexpr size_t size = 1 + sizeof...(TTail);

    template <size_t I>
    using At = typename std::template conditional_t<I == 0, Head, typename Tail::template At<I - 1>>;

    template <typename Func, size_t index = 0>
    static void ALWAYS_INLINE forEach(Func && func)
    {
        func.template operator()<Head, index>();
        Tail::template forEach<Func, index + 1>(std::forward<Func>(func));
    }
};

/// Append Type to TypeList
/// Usage:
///     using TypeListWithType = typename AppendToTypeList<Type, ConcreteTypeList>::Type;
template <typename TypeToPrepend, typename List, typename ... Types>
struct PrependToTypeList
{
    using Type = typename PrependToTypeList<TypeToPrepend, typename List::Tail, Types ..., typename List::Head>::Type;
};

template <typename TypeToPrepend, typename ... Types>
struct PrependToTypeList<TypeToPrepend, TypeList<>, Types ...>
{
    using Type = TypeList<TypeToPrepend, Types ...>;
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
