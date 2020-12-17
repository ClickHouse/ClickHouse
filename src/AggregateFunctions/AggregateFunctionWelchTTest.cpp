#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTTest.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Moments.h>

#include "registerAggregateFunctions.h"


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace DB
{

namespace
{

struct WelchTTestData : public TTestMoments<Float64>
{
    static constexpr auto name = "welchTTest";

    std::pair<Float64, Float64> getResult() const
    {
        Float64 mean_x = x1 / nx;
        Float64 mean_y = y1 / ny;

        /// s_x^2, s_y^2

        /// The original formulae looks like  \frac{1}{size_x - 1} \sum_{i = 1}^{size_x}{(x_i - \bar{x}) ^ 2}
        /// But we made some mathematical transformations not to store original sequences.
        /// Also we dropped sqrt, because later it will be squared later.

        Float64 sx2 = (x2 + nx * mean_x * mean_x - 2 * mean_x * x1) / (nx - 1);
        Float64 sy2 = (y2 + ny * mean_y * mean_y - 2 * mean_y * y1) / (ny - 1);

        /// t-statistic
        Float64 t_stat = (mean_x - mean_y) / sqrt(sx2 / nx + sy2 / ny);

        /// degrees of freedom

        Float64 numerator_sqrt = sx2 / nx + sy2 / ny;
        Float64 numerator = numerator_sqrt * numerator_sqrt;

        Float64 denominator_x = sx2 * sx2 / (nx * nx * (nx - 1));
        Float64 denominator_y = sy2 * sy2 / (ny * ny * (ny - 1));

        Float64 degrees_of_freedom = numerator / (denominator_x + denominator_y);

        return {t_stat, getPValue(degrees_of_freedom, t_stat * t_stat)};
    }
};

AggregateFunctionPtr createAggregateFunctionWelchTTest(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertBinary(name, argument_types);
    assertNoParameters(name, parameters);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception("Aggregate function " + name + " only supports numerical types", ErrorCodes::BAD_ARGUMENTS);

    return std::make_shared<AggregateFunctionTTest<WelchTTestData>>(argument_types);
}

}

void registerAggregateFunctionWelchTTest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("welchTTest", createAggregateFunctionWelchTTest);
}

}
