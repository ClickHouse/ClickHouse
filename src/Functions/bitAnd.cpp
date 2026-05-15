#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename A, typename B>
struct BitAndImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static constexpr bool allow_fixed_string = true;
    static constexpr bool allow_string_integer = false;
    static constexpr auto signature =
        /// `BinaryArithmeticOverloadResolver<..., true, false>` (4th param =
        /// `valid_on_float_arguments=false`) rejects floats at runtime — the
        /// signature must mirror that so a `Float` argument is caught during
        /// analysis as `ILLEGAL_TYPE_OF_ARGUMENT`, not later as
        /// `LOGICAL_ERROR` from the executor's "incorrect data types" branch.
        "(A : Integer, B : Integer) -> nativeNumber(maxBits(A, B), anySigned(A, B), 0)"
        " OR (F : FixedString, FixedString) -> F";

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) & static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BitAndImpl expected an integral type");
        return b.CreateAnd(left, right);
    }
#endif
};

struct NameBitAnd { static constexpr auto name = "bitAnd"; };
using FunctionBitAnd = BinaryArithmeticOverloadResolver<BitAndImpl, NameBitAnd, true, false>;

}

REGISTER_FUNCTION(BitAnd)
{
    FunctionDocumentation::Description description = "Performs bitwise AND operation between two values.";
    FunctionDocumentation::Syntax syntax = "bitAnd(a, b)";
    FunctionDocumentation::Arguments arguments = {
        {"a", "First value.", {"(U)Int*", "Float*"}},
        {"b", "Second value.", {"(U)Int*", "Float*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the result of bitwise operation `a AND b`"};
    FunctionDocumentation::Examples examples = {{"Usage example",
        R"(
CREATE TABLE bits
(
    `a` UInt8,
    `b` UInt8
)
ENGINE = Memory;

INSERT INTO bits VALUES (0, 0), (0, 1), (1, 0), (1, 1);

SELECT
    a,
    b,
    bitAnd(a, b)
FROM bits
        )",
        R"(
┌─a─┬─b─┬─bitAnd(a, b)─┐
│ 0 │ 0 │            0 │
│ 0 │ 1 │            0 │
│ 1 │ 0 │            0 │
│ 1 │ 1 │            1 │
└───┴───┴──────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitAnd>(documentation);
}

}
