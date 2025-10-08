#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Core/AccurateComparison.h>
#include <Functions/LeastGreatestGeneric.h>


namespace DB
{

template <typename A, typename B>
struct LeastBaseImpl
{
    using ResultType = NumberTraits::ResultOfLeast<A, B>;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        /** gcc 4.9.2 successfully vectorizes a loop from this function. */
        return static_cast<Result>(a) < static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
        {
            /// Follows the IEEE-754 semantics for minNum, except for handling of signaling NaNs. This match's the behavior of libc fmin.
            return b.CreateMinNum(left, right);
        }

        auto * compare_value = is_signed ? b.CreateICmpSLT(left, right) : b.CreateICmpULT(left, right);
        return b.CreateSelect(compare_value, left, right);
    }
#endif
};

template <typename A, typename B>
struct LeastSpecialImpl
{
    using ResultType = std::make_signed_t<A>;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::lessOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// ???
#endif
};

template <typename A, typename B>
using LeastImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, LeastBaseImpl<A, B>, LeastSpecialImpl<A, B>>;

struct NameLeast { static constexpr auto name = "least"; };
using FunctionLeast = FunctionBinaryArithmetic<LeastImpl, NameLeast>;

REGISTER_FUNCTION(Least)
{
    FunctionDocumentation::Description description = R"(
Returns the smallest value among the arguments.
`NULL` arguments are ignored.

- For arrays, returns the lexicographically least array.
- For DateTime types, the result type is promoted to the largest type (e.g., DateTime64 if mixed with DateTime32).

:::note Use setting `least_greatest_legacy_null_behavior` to change `NULL` behavior
Version [24.12](/whats-new/changelog/2024#a-id2412a-clickhouse-release-2412-2024-12-19) introduced a backwards-incompatible change such that `NULL` values are ignored, while previously it returned `NULL` if one of the arguments was `NULL`.
To retain the previous behavior, set setting `least_greatest_legacy_null_behavior` (default: `false`) to `true`.
:::
    )";
    FunctionDocumentation::Syntax syntax = "least(x1[, x2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"x1[, x2, ...]", "A single value or multiple values to compare. All arguments must be of comparable types.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the least value among the arguments, promoted to the largest compatible type.", {"Any"}};
    FunctionDocumentation::Examples examples = {
    {
        "Numeric types",
        R"(
SELECT least(1, 2, toUInt8(3), 3.) AS result, toTypeName(result) AS type;
-- The type returned is a Float64 as the UInt8 must be promoted to 64 bit for the comparison.
        )",
        R"(
┌─result─┬─type────┐
│      1 │ Float64 │
└────────┴─────────┘
        )"
    },
    {
        "Arrays",
        R"(
SELECT least(['hello'], ['there'], ['world']);
        )",
        R"(
┌─least(['hell⋯ ['world'])─┐
│ ['hello']                │
└──────────────────────────┘
        )"
    },
    {
        "DateTime types",
        R"(
SELECT least(toDateTime32(now() + toIntervalDay(1)), toDateTime64(now(), 3));
-- The type returned is a DateTime64 as the DateTime32 must be promoted to 64 bit for the comparison.
        )",
        R"(
┌─least(toDate⋯(now(), 3))─┐
│  2025-05-27 15:55:20.000 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Conditional;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<LeastGreatestOverloadResolver<LeastGreatest::Least, FunctionLeast>>(documentation, FunctionFactory::Case::Insensitive);
}

}
