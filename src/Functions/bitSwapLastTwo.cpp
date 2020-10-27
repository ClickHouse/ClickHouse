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

    /// Working with UInt8: last bit = can be true, previous = can be false (Like src/Storages/MergeTree/BoolMask.h).
    /// This function provides "NOT" operation for BoolMasks by swapping last two bits ("can be true" <-> "can be false").
    template <typename A>
    struct BitSwapLastTwoImpl
    {
        using ResultType = UInt8;
        static constexpr const bool allow_fixed_string = false;

        static inline ResultType NO_SANITIZE_UNDEFINED apply(A a)
        {
            if constexpr (!std::is_same_v<A, ResultType>)
                // Should be a logical error, but this function is callable from SQL.
                // Need to investigate this.
                throw DB::Exception("It's a bug! Only UInt8 type is supported by __bitSwapLastTwo.", ErrorCodes::BAD_ARGUMENTS);
            return static_cast<ResultType>(
                    ((static_cast<ResultType>(a) & 1) << 1) | ((static_cast<ResultType>(a) >> 1) & 1));
        }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception("__bitSwapLastTwo expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateOr(
                b.CreateShl(b.CreateAnd(arg, 1), 1),
                b.CreateAnd(b.CreateLShr(arg, 1), 1)
                );
    }
#endif
    };

    struct NameBitSwapLastTwo { static constexpr auto name = "__bitSwapLastTwo"; };
    using FunctionBitSwapLastTwo = FunctionUnaryArithmetic<BitSwapLastTwoImpl, NameBitSwapLastTwo, true>;

    template <> struct FunctionUnaryArithmeticMonotonicity<NameBitSwapLastTwo>
    {
        static bool has() { return false; }
        static IFunction::Monotonicity get(const Field &, const Field &)
        {
            return {};
        }
    };

    void registerFunctionBitSwapLastTwo(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionBitSwapLastTwo>();
    }

}
