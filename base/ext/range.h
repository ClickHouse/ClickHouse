#pragma once

#include <type_traits>
#include <boost/range/counting_range.hpp>
#include <boost/range/adaptor/transformed.hpp>


namespace ext
{
    /// For loop adaptor which is used to iterate through a half-closed interval [begin, end).
    template <typename BeginType, typename EndType>
    inline auto range(BeginType begin, EndType end)
    {
        using CommonType = typename std::common_type<BeginType, EndType>::type;
        return boost::counting_range<CommonType>(begin, end);
    }

    template <typename Type>
    inline auto range(Type end)
    {
        return range<Type, Type>(static_cast<Type>(0), end);
    }

    /// The same as range(), but every value is casted statically to a specified `ValueType`.
    /// This is useful to iterate through all constants of a enum.
    template <typename ValueType, typename BeginType, typename EndType>
    inline auto range_with_static_cast(BeginType begin, EndType end)
    {
        using CommonType = typename std::common_type<BeginType, EndType>::type;
        if constexpr (std::is_same_v<ValueType, CommonType>)
            return boost::counting_range<CommonType>(begin, end);
        else
            return boost::counting_range<CommonType>(begin, end)
                | boost::adaptors::transformed([](CommonType x) -> ValueType { return static_cast<ValueType>(x); });
    }

    template <typename ValueType, typename EndType>
    inline auto range_with_static_cast(EndType end)
    {
        return range_with_static_cast<ValueType, EndType, EndType>(static_cast<EndType>(0), end);
    }
}
