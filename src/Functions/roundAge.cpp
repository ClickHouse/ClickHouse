#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{

template <typename A>
struct RoundAgeImpl
{
    using ResultType = UInt8;
    static constexpr const bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    static inline ResultType apply(A x)
    {
        return x < 1 ? 0
            : (x < 18 ? 17
            : (x < 25 ? 18
            : (x < 35 ? 25
            : (x < 45 ? 35
            : (x < 55 ? 45
            : 55)))));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameRoundAge { static constexpr auto name = "roundAge"; };
using FunctionRoundAge = FunctionUnaryArithmetic<RoundAgeImpl, NameRoundAge, false>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundAge> : PositiveMonotonicity {};

void registerFunctionRoundAge(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRoundAge>();
}

}
