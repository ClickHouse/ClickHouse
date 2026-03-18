#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>

namespace DB
{
namespace
{
    struct Max2Name
    {
        static constexpr auto name = "max2";
    };

    template <typename T>
    T max(T a, T b)
    {
        return a > b ? a : b;
    }

    using FunctionMax2 = FunctionMathBinaryFloat64<BinaryFunctionVectorized<Max2Name, max>>;
}

REGISTER_FUNCTION(Max2)
{
    factory.registerFunction<FunctionMax2>({}, FunctionFactory::Case::Insensitive);
}
}
