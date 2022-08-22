#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>

namespace DB
{
namespace
{
    struct Atan2Name
    {
        static constexpr auto name = "atan2";
    };
    using FunctionAtan2 = FunctionMathBinaryFloat64<BinaryFunctionVectorized<Atan2Name, atan2>>;

}

void registerFunctionAtan2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAtan2>(FunctionFactory::CaseInsensitive);
}

}
