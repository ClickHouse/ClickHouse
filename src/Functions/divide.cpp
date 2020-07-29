#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Result, typename A, typename B>
inline Result applySpecial(A /*a*/, B /*b*/)
{
    throw Exception("DivideFloatingImpl are not implemented for big integers", ErrorCodes::LOGICAL_ERROR);
}

template <typename A, typename B>
struct DivideFloatingImpl
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_decimal = true;
    static const constexpr bool allow_fixed_string = false;
    static constexpr bool is_special = is_big_int_v<A> || is_big_int_v<B>;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        if constexpr (is_special)
            return applySpecial<Result>(a, b);
        else
            return static_cast<Result>(a) / b;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (left->getType()->isIntegerTy())
            throw Exception("DivideFloatingImpl expected a floating-point type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateFDiv(left, right);
    }
#endif
};

struct NameDivide { static constexpr auto name = "divide"; };
using FunctionDivide = FunctionBinaryArithmetic<DivideFloatingImpl, NameDivide>;

void registerFunctionDivide(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDivide>();
}

}
