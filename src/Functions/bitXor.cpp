#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Result, typename A, typename B>
inline Result applySpecial(A a, B b)
{
    if constexpr (std::is_same_v<A, UInt8>)
        return static_cast<Result>(static_cast<UInt16>(a)) ^ static_cast<Result>(b);
    else
        return static_cast<Result>(a) ^ static_cast<Result>(static_cast<UInt16>(b));
}

template <typename A, typename B>
struct BitXorImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static constexpr bool allow_fixed_string = true;
    static constexpr bool is_special = is_big_int_v<ResultType> &&
                                       (std::is_same_v<A, UInt8> || std::is_same_v<B, UInt8>);

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        if constexpr (is_special)
            return applySpecial<ResultType>(a, b);
        else
            return static_cast<Result>(a) ^ static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitXorImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateXor(left, right);
    }
#endif
};

struct NameBitXor { static constexpr auto name = "bitXor"; };
using FunctionBitXor = FunctionBinaryArithmetic<BitXorImpl, NameBitXor, true>;

void registerFunctionBitXor(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitXor>();
}

}
