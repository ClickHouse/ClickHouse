#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/GCDLCMImpl.h>

#include <boost/integer/common_factor.hpp>


namespace
{

template <typename T>
constexpr T abs(T value) noexcept
{
    if constexpr (std::is_signed_v<T>)
    {
        if (value >= 0 || value == std::numeric_limits<T>::min())
            return value;
        return -value;
    }
    else
        return value;
}

}


namespace DB
{

namespace
{

struct NameLCM { static constexpr auto name = "lcm"; };

template <typename A, typename B>
struct LCMImpl : public GCDLCMImpl<A, B, LCMImpl<A, B>, NameLCM>
{
    using ResultType = typename GCDLCMImpl<A, B, LCMImpl<A, B>, NameLCM>::ResultType;

    static ResultType applyImpl(A a, B b)
    {
        using Int = typename NumberTraits::ToInteger<ResultType>::Type;
        using Unsigned = make_unsigned_t<Int>;

        /** It's tempting to use std::lcm function.
          * But it has undefined behaviour on overflow.
          * And assert in debug build.
          * We need some well defined behaviour instead
          * (example: throw an exception or overflow in implementation specific way).
          */

        Unsigned val1 = abs<Int>(a) / boost::integer::gcd(Int(a), Int(b)); // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
        Unsigned val2 = abs<Int>(b);

        /// Overflow in implementation specific way.
        return ResultType(val1 * val2);
    }
};

using FunctionLCM = BinaryArithmeticOverloadResolver<LCMImpl, NameLCM, false, false>;

}

REGISTER_FUNCTION(LCM)
{
    factory.registerFunction<FunctionLCM>();
}

}
