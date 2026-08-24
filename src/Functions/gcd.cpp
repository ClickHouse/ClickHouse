#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/GCDLCMImpl.h>

#include <boost/integer/common_factor.hpp>


namespace DB
{

namespace
{

struct NameGCD { static constexpr auto name = "gcd"; };

template <typename A, typename B>
struct GCDImpl : public GCDLCMImpl<A, B, GCDImpl<A, B>, NameGCD>
{
    using ResultType = typename GCDLCMImpl<A, B, GCDImpl, NameGCD>::ResultType;

    static ResultType applyImpl(A a, B b)
    {
        using Int = typename NumberTraits::ToInteger<ResultType>::Type;
        return boost::integer::gcd(Int(a), Int(b)); // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
    }
};

using FunctionGCD = BinaryArithmeticOverloadResolver<GCDImpl, NameGCD, false, false>;

}

REGISTER_FUNCTION(GCD)
{
    FunctionDocumentation::Description description = R"(
    Returns the greatest common divisor of two values a and b.

    An exception is thrown when dividing by zero or when dividing a minimal
    negative number by minus one.
    )";
    FunctionDocumentation::Syntax syntax = "gcd(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "First integer"};
    FunctionDocumentation::Argument argument2 = {"y", "Second integer"};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = {"The greatest common divisor of `x` and `y`."};
    FunctionDocumentation::Example example1 = {"Usage example", "SELECT gcd(12, 18)", "6"};
    FunctionDocumentation::Examples examples = {example1};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGCD>(documentation);
}

}
