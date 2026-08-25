#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>


namespace DB
{
namespace
{

template <typename A, typename B>
struct ModuloOrZeroImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if constexpr (is_floating_point<ResultType>)
        {
            /// This computation is similar to `fmod` but the latter is not inlined and has 40 times worse performance.
            return ResultType(a) - trunc(ResultType(a) / ResultType(b)) * ResultType(b);
        }
        else
        {
            if (unlikely(divisionLeadsToFPE(a, b)))
                return 0;

            return ModuloImpl<A, B>::template apply<Result>(a, b);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

struct NameModuloOrZero { static constexpr auto name = "moduloOrZero"; };
using FunctionModuloOrZero = BinaryArithmeticOverloadResolver<ModuloOrZeroImpl, NameModuloOrZero>;

}

REGISTER_FUNCTION(ModuloOrZero)
{
    FunctionDocumentation::Description description = R"(
Like modulo but returns zero when the divisor is zero, as opposed to an
exception with the modulo function.
    )";
    FunctionDocumentation::Syntax syntax = "moduloOrZero(a, b)";
    FunctionDocumentation::Arguments arguments =
    {
        {"a", "The dividend.", {"(U)Int*", "Float*"}},
        {"b", "The divisor (modulus).", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the remainder of a % b, or `0` when the divisor is `0`."};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT moduloOrZero(5, 0)", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {20, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionModuloOrZero>(documentation);
}

}
