#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

#include <numbers>

namespace DB
{
namespace
{

struct Log10Name { static constexpr auto name = "log10"; };

#if USE_FASTOPS
void log10Kernel(const double * src, size_t size, double * dst) { fastNaturalLogScaled(src, size, dst, std::numbers::log10e); }
using FunctionLog10 = FunctionMathUnary<VectorizedFloat64Impl<Log10Name, log10Kernel>>;
#else
using FunctionLog10 = FunctionMathUnary<UnaryFunctionVectorized<Log10Name, log10>>;
#endif

}

REGISTER_FUNCTION(Log10)
{
    FunctionDocumentation::Description description = R"(
Returns the decimal logarithm of the argument.
)";
    FunctionDocumentation::Syntax syntax = "log10(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Number for which to compute the decimal logarithm of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the decimal logarithm of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT round(log10(100));", "2"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLog10>(documentation, FunctionFactory::Case::Insensitive);
}

}
