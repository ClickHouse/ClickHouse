#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename A, typename B>
struct BitRotateLeftImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Bit rotate is not implemented for big integers");
        else
            return (static_cast<Result>(a) << static_cast<Result>(b))
                | (static_cast<Result>(a) >> ((sizeof(Result) * 8) - static_cast<Result>(b)));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BitRotateLeftImpl expected an integral type");
        auto * size = llvm::ConstantInt::get(left->getType(), left->getType()->getPrimitiveSizeInBits());
        /// XXX how is this supposed to behave in signed mode?
        return b.CreateOr(b.CreateShl(left, right), b.CreateLShr(left, b.CreateSub(size, right)));
    }
#endif
};

struct NameBitRotateLeft { static constexpr auto name = "bitRotateLeft"; };
using FunctionBitRotateLeft = BinaryArithmeticOverloadResolver<BitRotateLeftImpl, NameBitRotateLeft, true, false>;

}

REGISTER_FUNCTION(BitRotateLeft)
{
    FunctionDocumentation::Description description = "Rotate bits left by a certain number of positions. Bits that fall off wrap around to the right.";
    FunctionDocumentation::Syntax syntax = "bitRotateLeft(a, N)";
    FunctionDocumentation::Arguments arguments = {
        {"a", "A value to rotate.", {"(U)Int8/16/32/64"}},
        {"N", "The number of positions to rotate left.", {"UInt8/16/32/64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the rotated value with type equal to that of `a`.", {"(U)Int8/16/32/64"}};
    FunctionDocumentation::Examples examples = {{"Usage example",
        R"(
SELECT 99 AS a, bin(a), bitRotateLeft(a, 2) AS a_rotated, bin(a_rotated);
        )",
        R"(
┌──a─┬─bin(a)───┬─a_rotated─┬─bin(a_rotated)─┐
│ 99 │ 01100011 │       141 │ 10001101       │
└────┴──────────┴───────────┴────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitRotateLeft>(documentation);
}

}
