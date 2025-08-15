#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename A, typename B>
struct DivideFloatingImpl
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        return static_cast<Result>(a) / static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (left->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DivideFloatingImpl expected a floating-point type");
        return b.CreateFDiv(left, right);
    }
#endif
};

template <typename A, typename B>
struct DivideFloatingOrNullImpl : DivideFloatingImpl<A, B>
{
#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameDivide { static constexpr auto name = "divide"; };
using FunctionDivide = BinaryArithmeticOverloadResolver<DivideFloatingImpl, NameDivide>;

REGISTER_FUNCTION(Divide)
{
    FunctionDocumentation::Description description = R"(
    Calculates the quotient of two values `a` and `b`. The result type is always [Float64](/sql-reference/data-types/float).
    Integer division is provided by the `intDiv` function.

    :::note
    Division by `0` returns `inf`, `-inf`, or `nan`.
    :::
    )";
    FunctionDocumentation::Syntax syntax = "divide(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "Dividend"};
    FunctionDocumentation::Argument argument2 = {"y", "Divisor"};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = "The quotient of x and y";
    FunctionDocumentation::Example example1 = {"Dividing two numbers", "SELECT divide(25,5) AS quotient, toTypeName(quotient)", "5 Float64"};
    FunctionDocumentation::Example example2 = {"Dividing by zero", "SELECT divide(25,0)", "inf"};
    FunctionDocumentation::Examples examples = {example1, example2};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionDivide>(documentation);
}

struct NameDivideOrNull { static constexpr auto name = "divideOrNull"; };
using FunctionDivideOrNull = BinaryArithmeticOverloadResolver<DivideFloatingOrNullImpl, NameDivideOrNull>;

REGISTER_FUNCTION(DivideOrNull)
{
    FunctionDocumentation::Description description = R"(
Same as `divide` but returns NULL when dividing by zero.
    )";
    FunctionDocumentation::Syntax syntax = "divideOrNull(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "Dividend"};
    FunctionDocumentation::Argument argument2 = {"y", "Divisor"};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = "The quotient of x and y, or NULL.";
    FunctionDocumentation::Example example1 = {"Dividing by zero", "SELECT divideOrNull(25, 0)", "\\N"};
    FunctionDocumentation::Examples examples = {example1};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionDivideOrNull>(documentation);
}

}
