#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{

struct ExpName { static constexpr auto name = "exp"; };

#if USE_FASTOPS

namespace
{
    struct Impl
    {
        static constexpr auto name = ExpName::name;
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
                NFastOps::Exp<true>(src, size, dst);
            }
        }
    };
}

using FunctionExp = FunctionMathUnary<Impl>;

#else
using FunctionExp = FunctionMathUnary<UnaryFunctionVectorized<ExpName, exp>>;
#endif

}

REGISTER_FUNCTION(Exp)
{
    FunctionDocumentation::Description description = R"(
Returns e raised to the power of `x`, where `x` is the given argument to the function.
)";
    FunctionDocumentation::Syntax syntax = "exp(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The exponent.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `e^x`", {"Float*"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT round(exp(-1), 4)",
        R"(
┌─round(exp(-1), 4)─┐
│            0.3679 │
└───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExp>(documentation, FunctionFactory::Case::Insensitive);
}

}
