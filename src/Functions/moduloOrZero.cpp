#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>


namespace DB
{
namespace
{

template <typename A, typename B>
struct ModuloOrZeroImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        if constexpr (std::is_floating_point_v<ResultType>)
        {
            /// This computation is similar to `fmod` but the latter is not inlined and has 40 times worse performance.
            return ResultType(a) - trunc(ResultType(a) / ResultType(b)) * ResultType(b);
        }
        else
        {
            if (unlikely(divisionLeadsToFPE(a, b)))
                return 0;

            return ModuloImpl<A, B>::template apply<Result>(a, b);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

struct NameModuloOrZero { static constexpr auto name = "moduloOrZero"; };
using FunctionModuloOrZero = BinaryArithmeticOverloadResolver<ModuloOrZeroImpl, NameModuloOrZero>;

}

void registerFunctionModuloOrZero(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModuloOrZero>();
}

}
