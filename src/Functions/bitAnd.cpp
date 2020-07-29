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
        return static_cast<Result>(static_cast<UInt16>(a)) & static_cast<Result>(b);
    else
        return static_cast<Result>(a) & static_cast<Result>(static_cast<UInt16>(b));
}

template <typename A, typename B>
struct BitAndImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;
    static constexpr const bool allow_fixed_string = true;
    static constexpr bool is_special = is_big_int_v<ResultType> &&
                                       (std::is_same_v<A, UInt8> || std::is_same_v<B, UInt8>);

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        if constexpr (is_special)
            return applySpecial<Result>(a, b);
        else
            return static_cast<Result>(a) & static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitAndImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateAnd(left, right);
    }
#endif
};

struct NameBitAnd { static constexpr auto name = "bitAnd"; };
using FunctionBitAnd = FunctionBinaryArithmetic<BitAndImpl, NameBitAnd, true>;

void registerFunctionBitAnd(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitAnd>();
}

}
