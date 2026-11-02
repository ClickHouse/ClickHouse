#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/GCDLCMImpl.h>

#include <boost/integer/common_factor.hpp>


namespace abs_impl
{

template <typename T>
constexpr T abs(T value) noexcept
{
    if constexpr (std::is_signed_v<T>)
    {
        if (value >= 0 || value == std::numeric_limits<T>::min())
            return value;
        return static_cast<T>(-value);
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

        Unsigned val1 = abs_impl::abs<Int>(a) / boost::integer::gcd(Int(a), Int(b)); // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
        Unsigned val2 = abs_impl::abs<Int>(b);

        /// Overflow in implementation specific way.
        return ResultType(val1 * val2);
    }
};

using FunctionLCM = BinaryArithmeticOverloadResolver<LCMImpl, NameLCM, false, false>;

}

REGISTER_FUNCTION(LCM)
{
    FunctionDocumentation::Description description = R"(
Returns the least common multiple of two values `x` and `y`.

An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.
    )";
    FunctionDocumentation::Syntax syntax = "lcm(x, y)";
    FunctionDocumentation::Arguments arguments =
    {
        {"x", "First integer.", {"(U)Int*"}},
        {"y", "Second integer.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the least common multiple of `x` and `y`.", {"(U)Int*"}};
    FunctionDocumentation::Examples example = {{"Usage example", "SELECT lcm(6, 8)", "24"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionLCM>(documentation);
}

}
