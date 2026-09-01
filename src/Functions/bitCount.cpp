#include <base/bit_cast.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <bit>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
template <typename A>
struct BitCountImpl
{
    using ResultType = std::conditional_t<(sizeof(A) * 8 >= 256), UInt16, UInt8>;
    static constexpr bool allow_string_or_fixed_string = true;

    static ResultType apply(A a)
    {
        /// We count bits in the value representation in memory. For example, we support floats.
        /// We need to avoid sign-extension when converting signed numbers to larger type. So, uint8_t(-1) has 8 bits.

        if constexpr (is_big_int_v<A>)
        {
            ResultType res = 0;
            for (auto item : a.items)
                res += std::popcount(item);
            return res;
        }
        else if constexpr (std::is_same_v<A, UInt64> || std::is_same_v<A, Int64>)
            return static_cast<ResultType>(std::popcount(static_cast<UInt64>(a)));
        else if constexpr (std::is_same_v<A, UInt32> || std::is_same_v<A, Int32>)
            return static_cast<ResultType>(std::popcount(static_cast<UInt32>(a)));
        else if constexpr (std::is_same_v<A, Int16>)
            return static_cast<ResultType>(std::popcount(static_cast<UInt16>(a)));
        else if constexpr (std::is_same_v<A, Int8>)
            return static_cast<ResultType>(std::popcount(static_cast<uint8_t>(a)));
        else
            return static_cast<ResultType>(std::popcount(bit_cast<uint64_t>(a)));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        const auto & type = arg->getType();
        llvm::Value * int_value = nullptr;

        if (type->isIntegerTy())
            int_value = arg;
        else if (type->isFloatTy())
            int_value = b.CreateBitCast(arg, llvm::Type::getInt32Ty(b.getContext()));
        else if (type->isDoubleTy())
            int_value = b.CreateBitCast(arg, llvm::Type::getInt64Ty(b.getContext()));
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BitCountImpl compilation expected native integer or floating-point type");

        auto * func_ctpop = llvm::Intrinsic::getDeclaration(b.GetInsertBlock()->getModule(), llvm::Intrinsic::ctpop, {int_value->getType()});
        llvm::Value * ctpop_value = b.CreateCall(func_ctpop, {int_value});
        return b.CreateZExtOrTrunc(ctpop_value, llvm::Type::getInt8Ty(b.getContext()));
    }
#endif
};

struct NameBitCount { static constexpr auto name = "bitCount"; };
using FunctionBitCount = FunctionUnaryArithmetic<BitCountImpl, NameBitCount, false /* is injective */>;

/// The function has no ranges of monotonicity.
template <> struct FunctionUnaryArithmeticMonotonicity<NameBitCount>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return {};
    }
};

REGISTER_FUNCTION(BitCount)
{
    FunctionDocumentation::Description description = "Calculates the number of bits set to one in the binary representation of a number.";
    FunctionDocumentation::Syntax syntax = "bitCount(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "An integer or float value.", {"(U)Int*", "Float*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the number of bits set to one in `x`. [`UInt8`](../data-types/int-uint.md).

:::note
The function does not convert the input value to a larger type ([sign extension](https://en.wikipedia.org/wiki/Sign_extension)).
For example: `bitCount(toUInt8(-1)) = 8`.
:::
)"};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT bin(333), bitCount(333);",
        R"(
┌─bin(333)─────────┬─bitCount(333)─┐
│ 0000000101001101 │             5 │
└──────────────────┴───────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBitCount>(documentation);
}

}
