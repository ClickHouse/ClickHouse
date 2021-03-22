#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename A, typename B>
struct BitOrImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static constexpr const bool allow_fixed_string = true;
    static constexpr bool need_uint8_cast = is_big_int_v<ResultType> && (std::is_same_v<A, UInt8> || std::is_same_v<B, UInt8>);

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        if constexpr (need_uint8_cast)
        {
            using CastA = std::conditional_t<std::is_same_v<A, UInt8>, uint8_t, A>;
            using CastB = std::conditional_t<std::is_same_v<B, UInt8>, uint8_t, B>;

            return static_cast<Result>(static_cast<CastA>(a)) | static_cast<Result>(static_cast<CastB>(b));
        }
        else
            return static_cast<Result>(a) | static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitOrImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateOr(left, right);
    }
#endif
};

struct NameBitOr { static constexpr auto name = "bitOr"; };
using FunctionBitOr = FunctionBinaryArithmetic<BitOrImpl, NameBitOr, true>;

void registerFunctionBitOr(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitOr>();
}

}
