#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

template <typename A, typename B>
struct MinusImpl
{
    using ResultType = typename NumberTraits::ResultOfSubtraction<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        {
            using CastA = std::conditional_t<is_floating_point<B>, B, A>;
            using CastB = std::conditional_t<is_floating_point<A>, A, B>;

            return static_cast<Result>(static_cast<CastA>(a)) - static_cast<Result>(static_cast<CastB>(b));
        }
        else
            return static_cast<Result>(a) - static_cast<Result>(b);
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static bool apply(A a, B b, Result & c)
    {
        return common::subOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};

struct NameMinus { static constexpr auto name = "minus"; };
using FunctionMinus = BinaryArithmeticOverloadResolver<MinusImpl, NameMinus>;

REGISTER_FUNCTION(Minus)
{
    FunctionDocumentation::Description description = R"(
Calculates the difference of two values `a` and `b`. The result is always signed.
Similar to plus, it is possible to subtract an integer from a date or date with time.
Additionally, subtraction between date with time is supported, resulting in the time difference between them.
    )";
    FunctionDocumentation::Syntax syntax = "minus(x, y)";
    FunctionDocumentation::Arguments arguments =
    {
        {"x", "Minuend."},
        {"y", "Subtrahend."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"x minus y"};
    FunctionDocumentation::Examples examples = {
        {"Subtracting two numbers", "SELECT minus(10, 5)", "5"},
        {"Subtracting an integer and a date", "SELECT minus(toDate('2025-01-01'),5)", "2024-12-27"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMinus>(documentation);
}

}
