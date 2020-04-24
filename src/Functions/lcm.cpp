#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template <typename A, typename B>
struct LCMImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;


    template <typename Result = ResultType>
    static inline Result apply([[maybe_unused]] A a, [[maybe_unused]] B b)
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
            throw Exception("LCM is not implemented for big integers", ErrorCodes::NOT_IMPLEMENTED);
        else {
            throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
            throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));
            return std::lcm(
                typename NumberTraits::ToInteger<Result>::Type(a),
                typename NumberTraits::ToInteger<Result>::Type(b));
        }
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
