#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Core/AccurateComparison.h>
#include <Functions/LeastGreatestGeneric.h>


namespace DB
{

template <typename A, typename B>
struct GreatestBaseImpl
{
    using ResultType = NumberTraits::ResultOfGreatest<A, B>;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) > static_cast<Result>(b) ?
               static_cast<Result>(a) : static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
        {
            /// Follows the IEEE-754 semantics for maxNum except for the handling of signaling NaNs. This matches the behavior of libc fmax.
            return b.CreateMaxNum(left, right);
        }

        auto * compare_value = is_signed ? b.CreateICmpSGT(left, right) : b.CreateICmpUGT(left, right);
        return b.CreateSelect(compare_value, left, right);
    }
#endif
};

template <typename A, typename B>
struct GreatestSpecialImpl
{
    using ResultType = make_unsigned_t<A>;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::greaterOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// ???
#endif
};

template <typename A, typename B>
using GreatestImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, GreatestBaseImpl<A, B>, GreatestSpecialImpl<A, B>>;

struct NameGreatest { static constexpr auto name = "greatest"; };
using FunctionGreatest = FunctionBinaryArithmetic<GreatestImpl, NameGreatest>;

REGISTER_FUNCTION(Greatest)
{
    FunctionDocumentation::Description description = R"(
Returns the greatest value among the arguments.

- For arrays, returns the lexicographically greatest array.
- For DateTime types, the result type is promoted to the largest type (e.g., DateTime64 if mixed with DateTime32).
    )";
    FunctionDocumentation::Syntax syntax = "greatest(x1[, x2, ..., xN])";
    FunctionDocumentation::Arguments arguments = {
        {"x1[, x2, ..., xN]", "One or multiple values to compare. All arguments must be of comparable types."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "The greatest value among the arguments, promoted to the largest compatible type.";
    FunctionDocumentation::Examples examples = {
        {"Numeric types", R"(
SELECT greatest(1, 2, toUInt8(3), 3.) AS result, toTypeName(result) AS type;
-- The type returned is a Float64 as the UInt8 must be promoted to 64 bit for the comparison.
    )",
    R"(
┌─result─┬─type────┐
│      3 │ Float64 │
└────────┴─────────┘
    )"},
        {"Arrays", R"(
SELECT greatest(['hello'], ['there'], ['world']);
    )",
    R"(
┌─greatest(['hello'], ['there'], ['world'])─┐
│ ['world']                                 │
└───────────────────────────────────────────┘
    )"},
        {"DateTime types", R"(
SELECT greatest(toDateTime32(now() + toIntervalDay(1)), toDateTime64(now(), 3));
-- The type returned is a DateTime64 as the DateTime32 must be promoted to 64 bit for the comparison.
    )",
    R"(
┌─greatest(toD⋯(now(), 3))─┐
│  2025-05-28 15:50:53.000 │
└──────────────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Conditional;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<LeastGreatestOverloadResolver<LeastGreatest::Greatest, FunctionGreatest>>(documentation, FunctionFactory::Case::Insensitive);
}

}
