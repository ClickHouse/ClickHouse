#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>


namespace DB
{
namespace
{
    struct RadiansName
    {
        static constexpr auto name = "radians";
    };

    Float64 radians(Float64 d)
    {
        Float64 radians = d * (M_PI / 180);
        return radians;
    }

    using FunctionRadians = FunctionMathUnary<UnaryFunctionVectorized<RadiansName, radians>>;
}

REGISTER_FUNCTION(Radians)
{
    factory.registerFunction<FunctionRadians>(FunctionFactory::CaseInsensitive);
}

}
