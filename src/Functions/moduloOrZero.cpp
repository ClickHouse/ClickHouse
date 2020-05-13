#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>


namespace DB
{

template <typename A, typename B>
struct ModuloOrZeroImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        if (unlikely(divisionLeadsToFPE(a, b)))
            return 0;

        return ModuloImpl<A, B>::template apply<Result>(a, b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

struct NameModuloOrZero { static constexpr auto name = "moduloOrZero"; };
using FunctionModuloOrZero = FunctionBinaryArithmetic<ModuloOrZeroImpl, NameModuloOrZero>;

void registerFunctionModuloOrZero(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModuloOrZero>();
}

}
