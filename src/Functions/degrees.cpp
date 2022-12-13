#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>


namespace DB
{
namespace
{
    struct DegreesName
    {
        static constexpr auto name = "degrees";
    };

    Float64 degrees(Float64 r)
    {
        Float64 degrees = r * (180 / M_PI);
        return degrees;
    }

    using FunctionDegrees = FunctionMathUnary<UnaryFunctionVectorized<DegreesName, degrees>>;
}

void registerFunctionDegrees(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDegrees>(FunctionFactory::CaseInsensitive);
}

}
