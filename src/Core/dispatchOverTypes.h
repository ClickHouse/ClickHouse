#pragma once

#include <Core/TypeId.h>
#include <Core/dispatchOverTypesImpl.h>

namespace DB
{
/**
 * If some TypeIndex element @a Type matches @p type and @p type satisfies constraints in @p D,
 * invokes @p f with <tt>TypePair<@p T, ReverseTypeId<@a Type >></tt>
 *
 * @return @c false if @p f was not called.
 * @return @p f return value otherwise.
 *
 * See tests/gtest_dispatchOverTypes.cpp for examples. You can think of this function as compile-time dispatching.
 */
template <Dispatch D = DISPATCH_ALL, class T = void>
constexpr bool dispatchOverType(TypeIndex type, auto && f)
{
    static_assert(!D.other, "You can't iterate over underlying types for Other: FixedString has no underlying type");
    return detail::dispatch<D, T, ReverseTypeId>(type, std::forward<decltype(f)>(f));
}

/// Same as dispatchOverType, but invokes @p f with matched type for @p left and @p right
template <Dispatch D = DISPATCH_ALL>
constexpr bool dispatchOverTypes(TypeIndex type, TypeIndex other, auto && f)
{
    static_assert(!D.other, "You can't iterate over underlying types for Other: FixedString has no underlying type");
    return detail::dispatch<D, ReverseTypeId>(type, other, std::forward<decltype(f)>(f));
}

/// Same as dispatchOverType, but @p f is invoked with ReverseDataTypeId
template <Dispatch D = DISPATCH_ALL_DT, class T = void>
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
template <Dispatch D = DISPATCH_ALL_DT>
constexpr bool dispatchOverDataTypes(TypeIndex left, TypeIndex right, auto && f)
{
    return detail::dispatch<D, ReverseDataTypeId>(left, right, std::forward<decltype(f)>(f));
}
}
