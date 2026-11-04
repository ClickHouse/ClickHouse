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
    /// See the comment in ModuloImpl.
    static constexpr bool no_vectorize = !is_floating_point<ResultType>;

    /// Documentation-only — see `PlusImpl::signature_documentation`.
    static constexpr auto signature_documentation = "(Any, Any) -> Any";

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
    /// Floats: the remainder of `ModuloImpl`, which cannot fail.
    static constexpr bool compilable = is_floating_point<ResultType>;

    static llvm::Value * compile(llvm::IRBuilder<> & builder, llvm::Value * left, llvm::Value * right, bool)
    {
        return JITIntegerDivision::compileFloatModulo(builder, left, right);
    }

    /// Integers up to 64 bits, from the original argument values like `modulo`: the rows where `divisionLeadsToFPE` give 0,
    /// so nothing is reported.
    static constexpr bool compilable_with_errors = is_integer<A> && is_integer<B> && !is_big_int_v<A> && !is_big_int_v<B>;

    static llvm::Value * compileWithErrors(llvm::IRBuilder<> & builder, llvm::Value * a, llvm::Value * divisor)
    {
        JITErrorValue error;
        auto * remainder = JITIntegerDivision::compileModulo<A, B, ResultType>(builder, a, divisor, error);
        return builder.CreateSelect(error.fails, llvm::Constant::getNullValue(remainder->getType()), remainder);
    }
#endif
};

struct NameModuloOrZero { static constexpr auto name = "moduloOrZero"; };
using FunctionModuloOrZero = BinaryArithmeticOverloadResolver<ModuloOrZeroImpl, NameModuloOrZero>;

}

REGISTER_FUNCTION(ModuloOrZero)
{
    FunctionDocumentation::Description description = R"(
Like modulo but returns zero instead of an exception for integer results when the operation would otherwise raise an
exception. For floating-point results, a zero divisor produces `NaN`.
    )";
    FunctionDocumentation::Syntax syntax = "moduloOrZero(a, b)";
    FunctionDocumentation::Arguments arguments =
    {
        {"a", "The dividend.", {"(U)Int*", "Float*"}},
        {"b", "The divisor (modulus).", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the remainder of `a % b`. For integer results, returns `0` when the operation would otherwise raise an exception.
For floating-point results, a zero divisor produces `NaN`.
    )"};
    FunctionDocumentation::Examples examples = {
        {"Integer zero divisor", "SELECT moduloOrZero(5, 0)", "0"},
        {"Floating-point zero divisor", "SELECT moduloOrZero(toFloat64(5), toFloat64(0))", "nan"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionModuloOrZero>(documentation);
}

}
