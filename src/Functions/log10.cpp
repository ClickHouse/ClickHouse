#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

#include <base/preciseExp10.h>

#include <cmath>
#include <numbers>

namespace DB
{
namespace
{

struct Log10Name { static constexpr auto name = "log10"; };

#if USE_FASTOPS
/// The logarithm of a power of ten is exact, as with `log10` of libm, and it is not below k for x >= 10^k, so that
/// `floor(log10(x))` counts the digits of x. The kernel reads `src` after it has written `dst`.
void log10Kernel(const double * src, size_t size, double * dst)
{
    fastNaturalLogScaled(src, size, dst, std::numbers::log10e);
    for (size_t i = 0; i < size; ++i)
    {
        const double k = std::round(dst[i]);
        if (std::abs(dst[i] - k) >= 1e-9)
            continue;
        const double power = preciseExp10(k);
        if (src[i] == power || (dst[i] < k && src[i] > power))
            dst[i] = k;
    }
}
using FunctionLog10 = FunctionMathUnary<VectorizedFloat64Impl<Log10Name, log10Kernel, /*ReadsSrcAfterWritingDst=*/ true>>;
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
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT log10(100);", "2"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLog10>(documentation, FunctionFactory::Case::Insensitive);
}

}
