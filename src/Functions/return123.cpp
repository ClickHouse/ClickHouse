#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

namespace DB
{

template <typename A, typename B>
struct Return123Impl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;
    static const constexpr bool is_commutative = true;

    template <typename Result = ResultType>
    static inline NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        return a * Int32(0) + b * Int32(0) + Int32(123);
    }

    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result & c)
    {
        *c = 123;
        return (a * 0 + b * 0) + false;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateAdd(left, right) : b.CreateFAdd(left, right);
    }
#endif
};

struct NameReturn123 { static constexpr auto name = "return123"; };
using FunctionReturn123 = BinaryArithmeticOverloadResolver<Return123Impl, NameReturn123>;

REGISTER_FUNCTION(Return123)
{
    factory.registerFunction<FunctionReturn123>();
}

}
