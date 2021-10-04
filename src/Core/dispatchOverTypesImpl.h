#pragma once

#include <Core/Types.h>
#include <base/TypePair.h>
#include <base/static_for.h>

namespace DB
{
struct Dispatch { bool ints = false , floats = false, decimals = false, datetimes = false, other = false; };

constexpr Dispatch DISPATCH_ALL = { true, true, true, true };
constexpr Dispatch DISPATCH_ALL_DT = { true, true, true, true, true };
}

namespace DB::detail
{
struct DispatchRet { bool func_was_called; bool func_ret; };

constexpr auto Ints = MakeCTArray(
    TypeIndex::UInt8, TypeIndex::UInt16, TypeIndex::UInt32, TypeIndex::UInt64, TypeIndex::UInt128, TypeIndex::UInt256,
    TypeIndex::Int8, TypeIndex::Int16, TypeIndex::Int32, TypeIndex::Int64 , TypeIndex::Int128, TypeIndex::Int256,
    TypeIndex::Enum8, TypeIndex::Enum16);

constexpr auto Floats = MakeCTArray(TypeIndex::Float32, TypeIndex::Float64);

constexpr auto Decimals = MakeCTArray(
    TypeIndex::Decimal32, TypeIndex::Decimal64, TypeIndex::Decimal128, TypeIndex::Decimal256);

constexpr auto DateTimes = MakeCTArray(TypeIndex::Date, TypeIndex::Date32, TypeIndex::DateTime, TypeIndex::DateTime64);

constexpr auto Other = MakeCTArray(TypeIndex::String, TypeIndex::FixedString, TypeIndex::UUID);

/**
 * If functor returns @c false in static_for, iteration will always continue.
 * We want to return function result after first match, so our functor should return a pair.
 * We also want to differentiate between "function returned false" and "function was not called"
 */
template <CTArray Container>
constexpr DispatchRet static_for_with_ret(auto && func)
{
    DispatchRet ret {.func_was_called = false, .func_ret = false };

    static_for<Container>([&ret, f = std::forward<decltype(func)>(func)](auto value)
    {
        const auto [should_stop_iteration, func_ret] = f(value);

        if (should_stop_iteration)
        {
            ret.func_was_called = true;
            ret.func_ret = func_ret;
            return true;
        }

        return false;
    });

    return ret;
}

template <Dispatch D, class F>
constexpr bool call(F && functor)
{
    if constexpr (D.ints)
        if (auto [match, ret] = static_for_with_ret<Ints>(std::forward<F>(functor)); match)
            return ret;

    if constexpr (D.floats)
        if (auto [match, ret] = static_for_with_ret<Floats>(std::forward<F>(functor)); match)
            return ret;

    if constexpr (D.decimals)
        if (auto [match, ret] = static_for_with_ret<Decimals>(std::forward<F>(functor)); match)
            return ret;

    if constexpr (D.datetimes)
        if (auto [match, ret] = static_for_with_ret<DateTimes>(std::forward<F>(functor)); match)
            return ret;

    if constexpr (D.other)
        if (auto [match, ret] = static_for_with_ret<Other>(std::forward<F>(functor)); match)
            return ret;

    return false;
}

template <Dispatch D, class T, template<TypeIndex> class Action>
constexpr bool dispatch(TypeIndex type, auto && f)
{
    return call<D>([type, f = std::forward<decltype(f)>(f)](auto index) -> DispatchRet
    {
        if (type != index) return {false, false};
        return {true, f(TypePair<T, Action<index>>())};
    });
}

template <Dispatch D, template<TypeIndex> class Action>
constexpr bool dispatch(TypeIndex type, TypeIndex other, auto && f)
{
    return call<D>([type, other, f = std::forward<decltype(f)>(f)](auto index) -> DispatchRet
    {
        if (type != index) return {false, false};
        return {true, dispatch<D, Action<index>, Action>(other, f)};
    });
}
}
