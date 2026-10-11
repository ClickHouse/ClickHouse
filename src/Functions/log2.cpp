#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

#include <numbers>

namespace DB
{
namespace
{

struct Log2Name { static constexpr auto name = "log2"; };

#if USE_FASTOPS
void log2Kernel(const double * src, size_t size, double * dst) { fastNaturalLogScaled(src, size, dst, std::numbers::log2e); }
using FunctionLog2 = FunctionMathUnary<VectorizedFloat64Impl<Log2Name, log2Kernel>>;
#else
using FunctionLog2 = FunctionMathUnary<UnaryFunctionVectorized<Log2Name, log2>>;
#endif

}

REGISTER_FUNCTION(Log2)
{
    FunctionDocumentation::Description description = R"(
Returns the binary logarithm of the argument.
)";
    FunctionDocumentation::Syntax syntax = "log2(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The number for which to compute the binary logarithm of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the binary logarithm of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT round(log2(8));", "3"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLog2>(documentation, FunctionFactory::Case::Insensitive);
}

}
