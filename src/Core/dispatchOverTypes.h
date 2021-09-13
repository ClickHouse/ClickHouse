#pragma once

#include <Core/Types.h>
#include <Core/TypeId.h>
#include <common/TypePair.h>

namespace DB
{
struct Dispatch { bool _int = false , _float = false, _decimal = false, _datetime = false; }; //NOLINT

namespace detail
{
constexpr auto Ints =
{
    TypeIndex::UInt8, TypeIndex::UInt16, TypeIndex::UInt32, TypeIndex::UInt64, TypeIndex::UInt128, TypeIndex::UInt256,
    TypeIndex::Int8, TypeIndex::Int16, TypeIndex::Int32, TypeIndex::Int64 , TypeIndex::Int128, TypeIndex::Int256,
    TypeIndex::Enum8, TypeIndex::Enum16
};

constexpr auto Floats = { TypeIndex::Float32, TypeIndex::Float64 };
constexpr auto Decimals = { TypeIndex::Decimal32, TypeIndex::Decimal64, TypeIndex::Decimal128, TypeIndex::Decimal256 };

constexpr auto DateTimes = { TypeIndex::Date, TypeIndex::Date32, TypeIndex::DateTime, TypeIndex::DateTime64 };

/// Unlike ordinary static_for, we need to return function's return value on first match (true or false)
template <auto Container>
constexpr bool static_for_with_policy(detail::returns_elem<Container.begin(), st> auto && func)
    requires detail::has_begin_end<Container>
{
    return static_for<Container.begin(), Container.end()>(
        std::forward<decltype(func)>(func));
}

template <Dispatch D, class F>
constexpr bool call(F && functor)
{
    if constexpr(D._int)
        if (static_for<Ints>(std::forward<F>(functor)))
            return true;

    if constexpr(D._float)
        if (static_for<Floats>(std::forward<F>(functor)))
            return true;

    if constexpr(D._decimal)
        if (static_for<Decimals>(std::forward<F>(functor)))
            return true;

    if constexpr(D._datetime)
        if (static_for<DateTimes>(std::forward<F>(functor)))
            return true;

    return false;
}
}

constexpr Dispatch DISPATCH_OVER_ALL = {._int = true, ._float = true, ._decimal = true, ._datetime = true };

/**
 * Compile time dispatching over underlying type.
 * Given a @p type, iterates over types specified by @p D, invokes @p f on type match.
 *
 * @param f Functor that is invoked when some type @a Type from @c TypeIndex that matches constraints from
 *  @a D equals @p type.
 *  @c f is invoked with <tt>TypePair<T, ReverseTypeId<MATCHED_TYPE>></tt>.
 *
 * @return @c false if @p f was not called (no type with given constraints satisfied @p type).
 * @return @p f return value otherwise
 *
 * @code{.cpp}
 * template <class T> void foo();
 *
 * TypeIndex t = TypeIndex::Int8;
 *
 * auto res = dispatchOverType(t, []<class Type>(TypePair<void, Type>) {
 *     foo<Type>(); /// will call foo<Int8>()
 * }); /// res == true
 *
 * constexpr Dispatch d { ._float = true }; // will iterate over TypeIndex::{Float32,  Float64};
 *
 * auto res2 = dispatchOverType<d>(t, []<class Type>(TypePair<void, Type>) {
 *     foo<Type>(); /// won't be called;
 * }); /// res2 == false
 * @endcode
 */
template <Dispatch D = DISPATCH_OVER_ALL, class T = void>
constexpr bool dispatchOverType(TypeIndex type, auto && f)
{
    return detail::call<D>([type, f = std::forward<decltype(f)>(f)](auto index)
    {
        if (type != index) return STATIC_CONTINUE;
        if (type == index) return {true, f(TypePair<T, ReverseTypeId<index>>())};
        return {false, false};
    });
}

/**
 * Same as dispatchOverType, but invokes @p f with
 * <tt>TypePair<ReverseTypeId<MATCHED_TYPE_LEFT>, ReverseTypeId<MATCHED_TYPE_RIGHT>></tt>.
 *
 * @code{.cpp}
 * template <class T, class V> void foo();
 *
 * TypeIndex left = TypeIndex::Int8;
 * TypeIndex right = TypeIndex::Float32;
 *
 * auto res = dispatchOverType(t, []<class L, class R>(TypePair<L, R>) {
 *     foo<L, R>(); /// will call foo<Int8, Float32>()
 * }); /// res == true
 *
 * constexpr Dispatch d { ._float = true }; // will iterate over TypeIndex::{Float32,  Float64};
 *
 * auto res2 = dispatchOverType<d>(t, []<class L, class R>(TypePair<L, R>) {
 *     foo<L, R>(); /// won't be called;
 * }); /// res2 == false
 * @endcode
 */
template <Dispatch D = DISPATCH_OVER_ALL>
constexpr bool dispatchOverTypes(TypeIndex type, TypeIndex other, auto && f)
{
    return detail::call<D>([type, other, f = std::forward<decltype(f)>(f)](auto index)
    {
        if (type == index) return callOnBasicType<D, ReverseTypeId<index>>(other, f);
        return STATIC_CONTINUE;
    });
}

class DataTypeDate;
class DataTypeDate32;
class DataTypeString;
class DataTypeFixedString;
class DataTypeUUID;
class DataTypeDateTime;
class DataTypeDateTime64;
template <typename T> class DataTypeEnum;
template <typename T> class DataTypeNumber;
template <is_decimal T> class DataTypeDecimal;

namespace detail
{
    // Type deduction
    // concept invokable_on_data_type
}

/**
 * Same as dispatchOverType, but:
 *
 * - @p f is invoked not with underlying type, but with data type, deduction rules follow.
 * - @p f must return @c bool. If @p f returned @c true, iteration will stop.
 *
 * Deduction rules for @c TypeIndex::@e T
 *
 * @a T is integral or floating-point -> DataTypeNumber<T>
 * @a T is Decimal -> DataTypeDecimal<T>
 * @a T is Enum8/Enum16 -> DataTypeEnum<Int8/Int16>
 *
 * @a T is Date -> DataTypeDate
 *
 * @a T is [Fixed]String -> DataType[Fixed]String
 *
 * @a T is Date[32] -> DataTypeDate[32]
 * @a T is DateTime[64] -> DataTypeDateTime[64]
 *
 * @a T is UUID -> DataTypeUUID
 */
template <class T = void>
constexpr bool dispatchOverDataType(TypeIndex type, auto && f, auto && ... args)
    requires std::is_same_v<bool, std::invoke_result_t<decltype(f), TypePair<T, ololo type>>>
{
    return dispatchOverType(type,
        [type, f = std::forward<decltype(f)>(f), ...args = std::forward<decltype(args)>(args)](auto value)
    {
        if (type == value) return f(TypePair<T, ololo type from value>, args...);
        return STATIC_CONTINUE;
    });
}

/// Same as dispatchOverDataType, but @p func is invoked with data types for @p left_type and @p right_type
template <typename F>
constexpr bool dispatchOverDataTypes(TypeIndex left_type, TypeIndex right_type, F && func)
{
    return dispatchOverDataType(left_type, [right_type, f = std::forward<F>(func)]<class T>(TypePair<void, T>)
    {
        return dispatchOverDataType(right_type, [f = std::forward<F>(f)]<class V>(TypePair<void, V>)
        {
            return f(TypePair<T, V>());
        });
    });
}
}
