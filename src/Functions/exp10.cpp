#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>
#include <base/preciseExp10.h>

namespace DB
{
namespace
{

struct Exp10Name { static constexpr auto name = "exp10"; };

#if USE_FASTOPS
void exp10Kernel(const double * src, size_t size, double * dst) { NFastOps::Exp10<true>(src, size, dst); }
using FunctionExp10 = FunctionMathUnary<VectorizedFloat64Impl<Exp10Name, exp10Kernel>>;
#else
using FunctionExp10 = FunctionMathUnary<UnaryFunctionVectorized<Exp10Name, preciseExp10>>;
#endif

}

REGISTER_FUNCTION(Exp10)
{
    FunctionDocumentation::Description description = R"(
Returns 10 to the power of the given argument.
)";
    FunctionDocumentation::Syntax syntax = "exp10(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The exponent.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 10^x", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT exp10(2);", "100"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExp10>(documentation);
}

}
