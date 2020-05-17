#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <numeric>


namespace DB
{

template <typename A, typename B>
struct LCMImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;


    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));
        return std::lcm(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// exceptions (and a non-trivial algorithm)
#endif
};

struct NameLCM { static constexpr auto name = "lcm"; };
using FunctionLCM = FunctionBinaryArithmetic<LCMImpl, NameLCM, false>;

void registerFunctionLCM(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLCM>();
}

}
