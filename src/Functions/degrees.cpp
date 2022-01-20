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

    template <typename T>
    Float64 degrees(T r)
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
