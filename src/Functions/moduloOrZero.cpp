#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template <typename A, typename B>
struct ModuloOrZeroImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
#if 1
        if constexpr (std::is_floating_point_v<ResultType> && (std::is_same_v<A, UInt256> || std::is_same_v<B, UInt256>))
        {
            throw Exception("Float vs UInt256 modulo is not implemented", ErrorCodes::NOT_IMPLEMENTED);
        }
#endif
        else if constexpr (std::is_floating_point_v<ResultType>)
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
using FunctionModuloOrZero = FunctionBinaryArithmetic<ModuloOrZeroImpl, NameModuloOrZero>;

void registerFunctionModuloOrZero(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModuloOrZero>();
}

}
