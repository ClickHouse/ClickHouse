#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Common/Exception.h>

namespace DB
{

template <typename A, typename B>
struct ModuloOrNullImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b, NullMap::value_type * m = nullptr)
    {
        if constexpr (std::is_floating_point_v<ResultType>)
        {
            /// This computation is similar to `fmod` but the latter is not inlined and has 40 times worse performance.
            return ResultType(a) - trunc(ResultType(a) / ResultType(b)) * ResultType(b);
        }
        else
        {
            if (unlikely(divisionLeadsToFPE(a, b)))
            {
                if (m)
                    *m = 1;

                return Result();
            }

            return ModuloImpl<A, B>::template apply<Result>(a, b);
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

struct NameModuloOrNull { static constexpr auto name = "moduloOrNull"; };
using FunctionModuloOrNull = BinaryArithmeticOverloadResolver<ModuloOrNullImpl, NameModuloOrNull>;


REGISTER_FUNCTION(ModuloOrNull)
{
    factory.registerFunction<FunctionModuloOrNull>();
}

}
