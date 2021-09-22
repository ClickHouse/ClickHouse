#pragma once

#include <Core/TypeId.h>
#include <common/TypePair.h>
#include <Core/dispatchOverTypesImpl.h>

namespace DB
{
/**
 * Compile time iteration. Given a @p type, iterates over types specified by @p D, invokes @p f on type match.
 *
 * @param f Functor that is invoked when some type @a Type from @c TypeIndex that matches constraints from
 *  @a D equals @p type.
 *  @c f is invoked with <tt>TypePair<T, ReverseTypeId<MATCHED_TYPE>></tt>.
 *
 * @return @c false if @p f was not called (no type with given constraints satisfied @p type).
 * @return @p f return value otherwise.
 *
 * See tests/gtest_dispatchOverTypes.cpp for examples
 */
template <Dispatch D = DISPATCH_OVER_ALL, class T = void>
constexpr bool dispatchOverType(TypeIndex type, auto && f)
{
    return detail::dispatch<D, T, ReverseTypeId>(type, std::forward<decltype(f)>(f));
}

/// Same as dispatchOverType, but invokes @p f with matched type for @p left and @p right
template <Dispatch D = DISPATCH_OVER_ALL>
constexpr bool dispatchOverTypes(TypeIndex type, TypeIndex other, auto && f)
{
    return detail::dispatch<D, ReverseTypeId>(type, other, std::forward<decltype(f)>(f));
}

/// Same as dispatchOverType, but @p f is invoked with ReverseDataTypeId
template <Dispatch D = DISPATCH_OVER_ALL, class T = void>
constexpr bool dispatchOverDataType(TypeIndex type, auto && f, auto && ... args)
{
    using F = decltype(f);

    auto capture_args = [f = std::forward<F>(f), ...args = std::forward<decltype(args)>(args)](auto type_pair)
    {
        return f(type_pair, args...);
    };

    return detail::dispatch<D, T, ReverseDataTypeId>(type, std::move(capture_args));
}

/// Same as dispatchOverDataType, but @p f is invoked with data types for @p left and @p right
template <Dispatch D = DISPATCH_OVER_ALL>
constexpr bool dispatchOverDataTypes(TypeIndex left, TypeIndex right, auto && f)
{
    return detail::dispatch<D, ReverseDataTypeId>(left, right, std::forward<decltype(f)>(f));
}
}
