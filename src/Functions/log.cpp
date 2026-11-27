#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>
#include <Functions/HighwayMath.h>

#include <cmath>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{

struct LogName { static constexpr auto name = "log"; };

struct Impl
{
    static constexpr auto name = LogName::name;
    static constexpr auto rows_per_iteration = 0;
    static constexpr bool always_returns_float64 = false;

    template <typename T>
    static void execute(const T * src, size_t size, T * dst)
    {
        if constexpr (std::is_same_v<T, BFloat16>)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function `{}` is not implemented for BFloat16", name);
        }
        else
        {
#if USE_HIGHWAY
            HighwayMath::log(src, size, dst);
#else
            for (size_t i = 0; i < size; ++i)
                dst[i] = std::log(src[i]);
#endif
        }
    }
};

using FunctionLog = FunctionMathUnary<Impl>;

}

REGISTER_FUNCTION(Log)
{
    FunctionDocumentation::Description description = R"(
Returns the natural logarithm of the argument.
)";
    FunctionDocumentation::Syntax syntax = "log(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The number for which to compute the natural logarithm of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the natural logarithm of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT log(10);", "2.3025850938475476"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLog>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("ln", "log", FunctionFactory::Case::Insensitive);
}

}
