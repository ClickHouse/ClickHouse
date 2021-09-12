#pragma once

#include <utility>
#include <Core/Types.h>
#include <Core/TypeId.h>
#include <common/TypeList.h>

namespace DB
{

template <typename T, typename U>
struct TypePair
{
    using LeftType = T;
    using RightType = U;
};

struct Dispatch { bool _int, _float, _decimal, _datetime; }; //NOLINT

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
}

template <typename T, Dispatch D>
constexpr bool callOnBasicType(TypeIndex number, auto && f)
{
    auto functor = [number, f = std::forward<decltype(f)>(f)](auto index) {
        if (number == index) f(TypePair<T, ReverseTypeId<index>>());
        return number == index;
    };

    if constexpr(D._int)
        if (static_for<Ints>(functor))
            return true;

    if constexpr(D._float)
        if (static_for<Floats>(functor))
            return true;

    if constexpr(D._decimal)
        if (static_for<Decimals>(functor))
            return true;

    if constexpr(D._datetime)
        if (static_for<DateTimes>(functor))
            return true;

    return false;
}

template <Dispatch D>
constexpr bool callOnBasicTypes(TypeIndex type, TypeIndex other, auto && f)
{
    auto functor = [type, other, f = std::forward<decltype(f)>(f)](auto index) {
        if (type == index) callOnBasicType<index, d>(other, f);
        return type == index;
    };

    if constexpr(D._int)
        if (static_for<Ints>(functor))
            return true;

    if constexpr(D._float)
        if (static_for<Floats>(functor))
            return true;

    if constexpr(D._decimal)
        if (static_for<Decimals>(functor))
            return true;

    if constexpr(D._datetime)
        if (static_for<DateTimes>(functor))
            return true;

    return false;
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


template <typename T, typename F, typename... ExtraArgs>
bool callOnIndexAndDataType(TypeIndex number, F && f, ExtraArgs && ... args)
{
    switch (number)
    {
        case TypeIndex::UInt8:          return f(TypePair<DataTypeNumber<UInt8>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::UInt16:         return f(TypePair<DataTypeNumber<UInt16>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::UInt32:         return f(TypePair<DataTypeNumber<UInt32>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::UInt64:         return f(TypePair<DataTypeNumber<UInt64>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::UInt128:        return f(TypePair<DataTypeNumber<UInt128>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::UInt256:        return f(TypePair<DataTypeNumber<UInt256>, T>(), std::forward<ExtraArgs>(args)...);

        case TypeIndex::Int8:           return f(TypePair<DataTypeNumber<Int8>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Int16:          return f(TypePair<DataTypeNumber<Int16>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Int32:          return f(TypePair<DataTypeNumber<Int32>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Int64:          return f(TypePair<DataTypeNumber<Int64>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Int128:         return f(TypePair<DataTypeNumber<Int128>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Int256:         return f(TypePair<DataTypeNumber<Int256>, T>(), std::forward<ExtraArgs>(args)...);

        case TypeIndex::Float32:        return f(TypePair<DataTypeNumber<Float32>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Float64:        return f(TypePair<DataTypeNumber<Float64>, T>(), std::forward<ExtraArgs>(args)...);

        case TypeIndex::Decimal32:      return f(TypePair<DataTypeDecimal<Decimal32>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Decimal64:      return f(TypePair<DataTypeDecimal<Decimal64>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Decimal128:     return f(TypePair<DataTypeDecimal<Decimal128>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Decimal256:     return f(TypePair<DataTypeDecimal<Decimal256>, T>(), std::forward<ExtraArgs>(args)...);

        case TypeIndex::Date:           return f(TypePair<DataTypeDate, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Date32:         return f(TypePair<DataTypeDate32, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::DateTime:       return f(TypePair<DataTypeDateTime, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::DateTime64:     return f(TypePair<DataTypeDateTime64, T>(), std::forward<ExtraArgs>(args)...);

        case TypeIndex::String:         return f(TypePair<DataTypeString, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::FixedString:    return f(TypePair<DataTypeFixedString, T>(), std::forward<ExtraArgs>(args)...);

        case TypeIndex::Enum8:          return f(TypePair<DataTypeEnum<Int8>, T>(), std::forward<ExtraArgs>(args)...);
        case TypeIndex::Enum16:         return f(TypePair<DataTypeEnum<Int16>, T>(), std::forward<ExtraArgs>(args)...);

        case TypeIndex::UUID:           return f(TypePair<DataTypeUUID, T>(), std::forward<ExtraArgs>(args)...);

        default:
            break;
    }

    return false;
}

template <typename F>
static bool callOnTwoTypeIndexes(TypeIndex left_type, TypeIndex right_type, F && func)
{
    return callOnIndexAndDataType<void>(left_type, [&](const auto & left_types) -> bool
    {
        using LeftTypes = std::decay_t<decltype(left_types)>;
        using LeftType = typename LeftTypes::LeftType;

        return callOnIndexAndDataType<void>(right_type, [&](const auto & right_types) -> bool
        {
            using RightTypes = std::decay_t<decltype(right_types)>;
            using RightType = typename RightTypes::LeftType;

            return std::forward<F>(func)(TypePair<LeftType, RightType>());
        });
    });
}

}
