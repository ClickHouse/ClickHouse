#pragma once

#include <DataTypes/NumberTraits.h>
#include <Common/Exception.h>
#include <base/extended_types.h>
#include <limits>
#include <type_traits>

#include "config.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
}


template <typename A, typename B, typename Impl, typename Name>
struct GCDLCMImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));

        using Int = typename NumberTraits::ToInteger<Result>::Type;

        if constexpr (is_signed_v<Result>)
        {
            /// gcd() internally uses std::abs()
            Int a_s = static_cast<Int>(a);
            Int b_s = static_cast<Int>(b);
            Int min = std::numeric_limits<Int>::lowest();
            Int max = std::numeric_limits<Int>::max();
            if (unlikely((a_s == min || a_s == max) || (b_s == min || b_s == max)))
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                                "Intermediate result overflow (signed a = {}, signed b = {}, min = {}, max = {})",
                                a_s, b_s, min, max);
        }

        return Impl::applyImpl(a, b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// exceptions (and a non-trivial algorithm)
#endif
};

}
