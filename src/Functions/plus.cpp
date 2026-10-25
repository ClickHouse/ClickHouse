#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

template <typename A, typename B>
struct PlusImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;
    static const constexpr bool is_commutative = true;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
        {
            using CastA = std::conditional_t<is_floating_point<B>, B, A>;
            using CastB = std::conditional_t<is_floating_point<A>, A, B>;

            return static_cast<Result>(static_cast<CastA>(a)) + static_cast<Result>(static_cast<CastB>(b));
        }
        else
            return static_cast<Result>(a) + static_cast<Result>(b);
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static bool apply(A a, B b, Result & c)
    {
        return common::addOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateAdd(left, right) : b.CreateFAdd(left, right);
    }
#endif
};

struct NamePlus { static constexpr auto name = "plus"; };
using FunctionPlus = BinaryArithmeticOverloadResolver<PlusImpl, NamePlus>;

REGISTER_FUNCTION(Plus)
{
    FunctionDocumentation::Description description = R"(
Calculates the sum of two values `x` and `y`. Alias: `x + y` (operator).
It is possible to add an integer and a date or date with time. The former
operation increments the number of days in the date, the latter operation
increments the number of seconds in the date with time.
    )";
    FunctionDocumentation::Syntax syntax = "plus(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "Left hand operand."};
    FunctionDocumentation::Argument argument2 = {"y", "Right hand operand."};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the sum of x and y"};
    FunctionDocumentation::Example example1 = {"Adding two numbers", "SELECT plus(5,5)", "10"};
    FunctionDocumentation::Example example2 = {"Adding an integer and a date", "SELECT plus(toDate('2025-01-01'),5)", "2025-01-06"};
    FunctionDocumentation::Examples examples = {example1, example2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPlus>(documentation);
}

}
