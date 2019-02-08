#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <boost/integer/common_factor.hpp>

namespace DB
{

template <typename A, typename B>
struct GCDImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));
        return boost::integer::gcd(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// exceptions (and a non-trivial algorithm)
#endif
};

struct NameGCD { static constexpr auto name = "gcd"; };
using FunctionGCD = FunctionBinaryArithmetic<GCDImpl, NameGCD, false>;

void registerFunctionGCD(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGCD>();
}

}
