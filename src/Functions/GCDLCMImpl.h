#pragma once

#include <DataTypes/NumberTraits.h>
#include <Common/Exception.h>
#include <numeric>
#include <limits>
#include <type_traits>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int DECIMAL_OVERFLOW;
}

template <class T>
inline constexpr bool is_gcd_lcm_implemeted = !is_big_int_v<T>;

template <typename A, typename B, typename Impl, typename Name>
struct GCDLCMImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline std::enable_if_t<!is_gcd_lcm_implemeted<Result>, Result>
    apply(A, B)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not implemented for big integers", Name::name);
    }

    template <typename Result = ResultType>
    static inline std::enable_if_t<is_gcd_lcm_implemeted<Result>, Result>
    apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));

        using Int = typename NumberTraits::ToInteger<Result>::Type;

        if constexpr (is_signed_v<Result>)
        {
            /// gcd() internally uses std::abs()
            Int a_s = static_cast<Int>(a);
            Int b_s = static_cast<Int>(b);
            Int min = std::numeric_limits<Int>::min();
            Int max = std::numeric_limits<Int>::max();
            if (unlikely((a_s == min || a_s == max) || (b_s == min || b_s == max)))
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Intermediate result overflow (signed a = {}, signed b = {}, min = {}, max = {})", a_s, b_s, min, max);
        }

        return Impl::applyImpl(a, b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// exceptions (and a non-trivial algorithm)
#endif
};

}
