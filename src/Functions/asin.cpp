#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct AsinName { static constexpr auto name = "asin"; };
using FunctionAsin = FunctionMathUnary<UnaryFunctionVectorized<AsinName, asin>>;

}

REGISTER_FUNCTION(Asin)
{
    factory.registerFunction<FunctionAsin>(FunctionDocumentation
        {
            .description=R"(
Calculates the arcsine of the argument.

Takes arbitrary numeric type, which includes floating point and integer numbers, as well as big integers and decimals and returns Float64.

For arguments in range [-1, 1] it returns the value in range of [-pi() / 2, pi() / 2].

It represents an inverse function to function 'sin' on this range:
[example:inverse]

It always returns Float64, even if the argument has Float32 type:
[example:float32]

For arguments outside of this range, it returns nan:
[example:nan]

Every self-respectful data scientist knows how to apply arcsine to improve ads click-through rate with ClickHouse.
For more details, see [https://en.wikipedia.org/wiki/Inverse_trigonometric_functions].
)",
            .examples{
                {"inverse", "SELECT asin(1.0) = pi() / 2, sin(asin(1)), asin(sin(1))", ""},
                {"float32", "SELECT toTypeName(asin(1.0::Float32))", ""},
                {"nan", "SELECT asin(1.1), asin(-2), asin(inf), asin(nan)", ""}},
            .categories{"Mathematical", "Trigonometric"}
        },
        FunctionFactory::Case::Insensitive);
}

}
