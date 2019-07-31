#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

#include "intDiv.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename A, typename B>
struct DivideIntegralOrZeroImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        if (unlikely(divisionLeadsToFPE(a, b)))
            return 0;

        if constexpr (!std::is_same_v<ResultType, NumberTraits::Error>)
            return DivideIntegralImpl<A, B>::apply(a, b);
        else
            throw Exception("Logical error: the types are not divisable", ErrorCodes::LOGICAL_ERROR);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

struct NameIntDivOrZero { static constexpr auto name = "intDivOrZero"; };
using FunctionIntDivOrZero = FunctionBinaryArithmetic<DivideIntegralOrZeroImpl, NameIntDivOrZero>;

void registerFunctionIntDivOrZero(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntDivOrZero>();
}

}
