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

struct TanhName { static constexpr auto name = "tanh"; };

#if USE_FASTOPS

    struct Impl
    {
        static constexpr auto name = TanhName::name;
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
                NFastOps::Tanh<>(src, size, dst);
            }
        }
    };

using FunctionTanh = FunctionMathUnary<Impl>;

#else

double tanh(double x)
{
    return 2 / (1.0 + exp(-2 * x)) - 1;
}

using FunctionTanh = FunctionMathUnary<UnaryFunctionVectorized<TanhName, tanh>>;
#endif

}

REGISTER_FUNCTION(Tanh)
{
    FunctionDocumentation::Description description = R"(
Returns the hyperbolic tangent.
)";
    FunctionDocumentation::Syntax syntax = "tanh(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The angle in radians. Values from the interval: -∞ < x < +∞.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns values from the interval: -1 < tanh(x) < 1", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT tanh(0)", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTanh>(documentation, FunctionFactory::Case::Insensitive);
}

}
