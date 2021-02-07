#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>

namespace DB
{
namespace
{

template <typename A>
struct RoundDurationImpl
{
    using ResultType = UInt16;
    static constexpr const bool allow_fixed_string = false;

    static inline ResultType apply(A x)
    {
        return x < 1 ? 0
            : (x < 10 ? 1
            : (x < 30 ? 10
            : (x < 60 ? 30
            : (x < 120 ? 60
            : (x < 180 ? 120
            : (x < 240 ? 180
            : (x < 300 ? 240
            : (x < 600 ? 300
            : (x < 1200 ? 600
            : (x < 1800 ? 1200
            : (x < 3600 ? 1800
            : (x < 7200 ? 3600
            : (x < 18000 ? 7200
            : (x < 36000 ? 18000
            : 36000))))))))))))));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameRoundDuration { static constexpr auto name = "roundDuration"; };
using FunctionRoundDuration = FunctionUnaryArithmetic<RoundDurationImpl, NameRoundDuration, false>;

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundDuration> : PositiveMonotonicity {};

void registerFunctionRoundDuration(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRoundDuration>();
}

}
