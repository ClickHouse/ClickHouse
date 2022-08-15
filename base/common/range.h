#pragma once

#include <boost/range/counting_range.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <type_traits>

namespace collections
{

namespace internal
{
    template <typename ResultType, typename CountingType, typename BeginType, typename EndType>
    auto rangeImpl(BeginType begin, EndType end)
    {
        if constexpr (std::is_same_v<ResultType, CountingType>)
            return boost::counting_range<CountingType>(static_cast<CountingType>(begin), static_cast<CountingType>(end));
        else
            return boost::counting_range<CountingType>(static_cast<CountingType>(begin), static_cast<CountingType>(end))
                | boost::adaptors::transformed([](CountingType x) { return static_cast<ResultType>(x); });
    }
}


/// For loop adaptor which is used to iterate through a half-closed interval [begin, end).
/// The parameters `begin` and `end` can have any integral or enum types.
template <typename BeginType,
        typename EndType,
        typename = std::enable_if_t<
            (std::is_integral_v<BeginType> || std::is_enum_v<BeginType>) &&
            (std::is_integral_v<EndType> || std::is_enum_v<EndType>) &&
            (!std::is_enum_v<BeginType> || !std::is_enum_v<EndType> || std::is_same_v<BeginType, EndType>), void>>
inline auto range(BeginType begin, EndType end)
{
    if constexpr (std::is_integral_v<BeginType> && std::is_integral_v<EndType>)
    {
        using CommonType = std::common_type_t<BeginType, EndType>;
        return internal::rangeImpl<CommonType, CommonType>(begin, end);
    }
    else if constexpr (std::is_enum_v<BeginType>)
    {
        return internal::rangeImpl<BeginType, std::underlying_type_t<BeginType>>(begin, end);
    }
    else
    {
        return internal::rangeImpl<EndType, std::underlying_type_t<EndType>>(begin, end);
    }
}


/// For loop adaptor which is used to iterate through a half-closed interval [0, end).
/// The parameter `end` can have any integral or enum type.
/// The same as range(0, end).
template <typename Type,
        typename = std::enable_if_t<std::is_integral_v<Type> || std::is_enum_v<Type>, void>>
inline auto range(Type end)
{
    if constexpr (std::is_integral_v<Type>)
        return internal::rangeImpl<Type, Type>(0, end);
    else
        return internal::rangeImpl<Type, std::underlying_type_t<Type>>(0, end);
}

}
