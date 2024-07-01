#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Working with UInt8: last bit = can be true, previous = can be false (Like src/Storages/MergeTree/BoolMask.h).
/// This function provides "NOT" operation for BoolMasks by swapping last two bits ("can be true" <-> "can be false").
template <typename A>
struct BitSwapLastTwoImpl
{
    using ResultType = UInt8;
    static constexpr const bool allow_string_or_fixed_string = false;

    static ResultType NO_SANITIZE_UNDEFINED apply([[maybe_unused]] A a)
    {
        if constexpr (!std::is_same_v<A, ResultType>)
            // Should be a logical error, but this function is callable from SQL.
            // Need to investigate this.
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "It's a bug! Only UInt8 type is supported by __bitSwapLastTwo.");

        auto little_bits = littleBits<A>(a);
        return static_cast<ResultType>(((little_bits & 1) << 1) | ((little_bits >> 1) & 1));
    }

#if USE_EMBEDDED_COMPILER
static constexpr bool compilable = true;

static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
{
    if (!arg->getType()->isIntegerTy())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "__bitSwapLastTwo expected an integral type");
    return b.CreateOr(
            b.CreateShl(b.CreateAnd(arg, 1), 1),
            b.CreateAnd(b.CreateLShr(arg, 1), 1)
            );
}
#endif
};

struct NameBitSwapLastTwo { static constexpr auto name = "__bitSwapLastTwo"; };
using FunctionBitSwapLastTwo = FunctionUnaryArithmetic<BitSwapLastTwoImpl, NameBitSwapLastTwo, true>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameBitSwapLastTwo>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

REGISTER_FUNCTION(BitSwapLastTwo)
{
    factory.registerFunction<FunctionBitSwapLastTwo>();
}

}
