#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/GCDLCMImpl.h>


namespace DB
{

namespace
{

struct NameGCD { static constexpr auto name = "gcd"; };

template <typename A, typename B>
struct GCDImpl : public GCDLCMImpl<A, B, GCDImpl<A, B>, NameGCD>
{
    using ResultType = typename GCDLCMImpl<A, B, GCDImpl, NameGCD>::ResultType;

    static ResultType applyImpl(A a, B b)
    {
        using Int = typename NumberTraits::ToInteger<ResultType>::Type;
        return std::gcd(Int(a), Int(b));
    }
};

using FunctionGCD = BinaryArithmeticOverloadResolver<GCDImpl, NameGCD, false, false>;

}

void registerFunctionGCD(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGCD>();
}

}
