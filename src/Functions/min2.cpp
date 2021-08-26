#include <Functions/FunctionMathBinaryFloat64.h>
#include <Functions/FunctionFactory.h>

template <typename T>
T min(T a, T b)
{
    return a < b ? a : b;
}

namespace DB
{
namespace
{
    struct Min2Name { static constexpr auto name = "min2"; };
    using FunctionMin2 = FunctionMathBinaryFloat64<BinaryFunctionVectorized<Min2Name, min>>;
}

void registerFunctionMin2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMin2>(FunctionFactory::CaseInsensitive);
}
}
